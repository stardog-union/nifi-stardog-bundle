package com.stardog.nifi;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.complexible.common.rdf.query.SPARQLUtil;
import com.complexible.common.rdf.query.SPARQLUtil.QueryType;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.UpdateQuery;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "stardog", "sparql", "read" })
@CapabilityDescription("Execute provided SPARQL update query. This processor can be scheduled to run on a timer, or cron expression, using the standard " +
                       "scheduling methods, or it can be triggered by an incoming FlowFile. If it is triggered by an incoming FlowFile, then " +
                       "attributes of that FlowFile will be available when evaluating the query but the contents of that file will not be used. ")
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class StardogUpdateQuery extends AbstractStardogQueryProcessor {

	public static final PropertyDescriptor QUERY_NAME =
			new PropertyDescriptor.Builder()
					.name("Query Name")
					.description("Stored SPARQL update query name")
					.required(false)
					.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.build();

	public static final PropertyDescriptor QUERY =
			new PropertyDescriptor.Builder()
					.name("Query")
					.description("SPARQL update query")
					.required(false)
					.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.build();

	private static final List<PropertyDescriptor> PROPERTIES =
			ImmutableList.<PropertyDescriptor>builder()
					.addAll(DEFAULT_PROPERTIES)
					.add(QUERY_NAME)
					.add(QUERY)
					.build();

	public StardogUpdateQuery() {
		super(QUERY_NAME, QUERY);
	}

	@Override
	protected void init(ProcessorInitializationContext context) {

	}

	@Override
	public Set<Relationship> getRelationships() {
		return DEFAULT_RELATIONSHIPS;
	}

	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return PROPERTIES;
	}

	@Override
	protected void customValidate(ValidationContext validationContext, Set<ValidationResult> results) {
		validateQueryName(validationContext, results);

		String queryStr = validationContext.getProperty(QUERY).getValue();
		if (queryStr != null && !queryStr.trim().startsWith("$")) {
			QueryType queryType = SPARQLUtil.getType(queryStr);

			if (queryType != QueryType.UPDATE) {
				String msg = String.format("Unsupported query type: %s", queryType);
				results.add(new ValidationResult.Builder().subject(QUERY.getDisplayName())
				                                          .valid(false)
				                                          .explanation(msg)
				                                          .build());
			}
		}
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile inputFile = getOptionalFlowFile(context, session);
		if (inputFile == null) {
			return;
		}

		Stopwatch stopwatch = Stopwatch.createStarted();

		ComponentLog logger = getLogger();

		try (Connection connection = connect(context)) {
			String queryStr = getQueryString(context, inputFile, connection);

			UpdateQuery query = connection.update(queryStr);

			getBindings(context, inputFile, connection).forEach(query::parameter);

			query.execute();

			logger.info("Update completed; transferring {} to 'success'", new Object[] { inputFile });
			session.getProvenanceReporter()
			       .modifyContent(inputFile, "Executed update query", stopwatch.elapsed(TimeUnit.MILLISECONDS));
			session.transfer(inputFile, REL_SUCCESS);
		}
		catch (Throwable t) {
			Throwable rootCause = Throwables.getRootCause(t);
			context.yield();
			logger.error("{} failed! Throwable exception {}; rolling back session", new Object[] { this, rootCause });
			session.transfer(inputFile, REL_FAILURE);
		}
	}
}
