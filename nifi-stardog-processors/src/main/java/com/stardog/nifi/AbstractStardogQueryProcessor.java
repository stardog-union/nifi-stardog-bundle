package com.stardog.nifi;

import java.util.Map;
import java.util.Set;

import com.complexible.common.rdf.rio.TurtleValueParser;
import com.complexible.stardog.Schemas;
import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.reasoning.ReasoningConnection;
import com.complexible.stardog.api.reasoning.SchemaManager;
import com.complexible.stardog.security.StardogAuthorizationException;
import com.stardog.stark.Namespaces;
import com.stardog.stark.Value;

import com.google.common.collect.Maps;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

public abstract class AbstractStardogQueryProcessor extends AbstractStardogProcessor {

	public static final PropertyDescriptor QUERY_TIMEOUT =
			new PropertyDescriptor.Builder()
					.name("Query Timeout")
					.description("The maximum amount of time allowed for a running query. Must be of format "
					             + "<duration> <TimeUnit> where <duration> is a non-negative integer and TimeUnit is a supported "
					             + "Time Unit, such as: nanos, millis, secs, mins, hrs, days. A value of zero means there is no limit.")
					.defaultValue("0 seconds")
					.required(true)
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
					.build();

	public static final PropertyDescriptor REASONING =
			new PropertyDescriptor.Builder()
					.name("Reasoning")
					.description("Enable reasoning for the query.")
					.required(true)
					.defaultValue("false")
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
					.build();

	public static final PropertyDescriptor REASONING_SCHEMA =
			new PropertyDescriptor.Builder()
					.name("Reasoning Schema")
					.description("Select the reasoning schema to be used if reasoning is enabled. This value takes effect " +
					             "only if reasoning option is enabled.")
					.required(false)
					.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.build();

	private final PropertyDescriptor mQueryNameDescriptor;

	private final PropertyDescriptor mQueryDescriptor;

	protected AbstractStardogQueryProcessor(PropertyDescriptor queryNameDescriptor, PropertyDescriptor queryDescriptor) {
		mQueryNameDescriptor = queryNameDescriptor;
		mQueryDescriptor = queryDescriptor;
	}

	@Override
	protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
		if (propertyDescriptorName != null &&
		    propertyDescriptorName.length() > 0 &&
		    // Close enough to a legit var name? Allows num, alpha, and _
		    propertyDescriptorName.matches("[\\w]+")) {

			return new PropertyDescriptor.Builder()
					.name(propertyDescriptorName)
					.displayName("Query parameter " + propertyDescriptorName)
					.description("The value of this parameter will be used in place of the SPARQL variable with the same name.")
					.dynamic(true)
					.required(false)
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.addValidator(Validator.VALID)
					.build();
		}
		else {
			return null;
		}
	}

	protected void validateQueryName(ValidationContext validationContext, Set<ValidationResult> results) {
		PropertyValue queryName = validationContext.getProperty(mQueryNameDescriptor);
		PropertyValue query = validationContext.getProperty(mQueryDescriptor);
		if (query.isSet() && queryName.isSet()) {
			results.add(new ValidationResult.Builder().valid(false)
			                                          .subject("Query")
			                                          .input(queryName.getValue())
			                                          .explanation("Cannot set both " +
			                                                       mQueryNameDescriptor.getDisplayName() +
			                                                       " and " +
			                                                       mQueryDescriptor.getDisplayName())
			                                          .build());
		}
		else if (!query.isSet() && !queryName.isSet()) {
			results.add(new ValidationResult.Builder().valid(false)
			                                          .subject("Query")
			                                          .explanation("Must set either " +
			                                                       mQueryNameDescriptor.getDisplayName() +
			                                                       " or " +
			                                                       mQueryDescriptor.getDisplayName())
			                                          .build());
		}
	}

	protected String getQueryString(ProcessContext context, FlowFile inputFile, Connection connection) {
		return context.getProperty(mQueryDescriptor).isSet()
		       ? context.getProperty(mQueryDescriptor).evaluateAttributeExpressions(inputFile).getValue()
		       : connection.admin()
		                   .getStoredQueries()
		                   .get(context.getProperty(mQueryNameDescriptor)
		                               .evaluateAttributeExpressions(inputFile)
		                               .getValue())
		                   .getQuery();
	}

	protected Map<String, Value> getBindings(ProcessContext context, FlowFile inputFile, Connection connection) {
		Map<String,Value> aParameters =  Maps.newHashMap();

		Namespaces namespaces = namespaces(connection);
		TurtleValueParser aParser = new TurtleValueParser(namespaces);

		for (Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
			PropertyDescriptor descriptor = entry.getKey();
			if (descriptor.isDynamic() && entry.getValue() != null) {
				try {
					Value value = aParser.parse(context.getProperty(descriptor.getName())
					                                   .evaluateAttributeExpressions(inputFile)
					                                   .getValue());
					aParameters.put(descriptor.getName(), value);
				}
				catch (Exception e) {
					throw new ProcessException("Invalid value for property '" + descriptor.getName() + "': " + entry.getValue());
				}
			}
		}
		return aParameters;
	}

	private Namespaces namespaces(Connection theConn) throws StardogException {
		Namespaces aNamespaces = Namespaces.DEFAULT;
		try {
			aNamespaces = theConn.namespaces();
		}
		catch (StardogAuthorizationException e) {
			// if there was a security error here because you could not read the namespaces, dont punt on the query
			// just dont use them.
			getLogger().info("Could not retrieve namespaces from database, proceeding without them.");
		}
		return aNamespaces;
	}

	protected String getSchema(ProcessContext context, FlowFile inputFile, boolean isReasoning) {
		PropertyValue schemaValue = context.getProperty(REASONING_SCHEMA).evaluateAttributeExpressions(inputFile);
		return schemaValue.isSet()
		       ? schemaValue.getValue()
		       : isReasoning
		         ? Schemas.DEFAULT
		         : Schemas.NULL;
	}

	protected void validateSchema(ValidationContext context, Set<ValidationResult> results) {
		PropertyValue schema = context.getProperty(REASONING_SCHEMA);
		if (schema.isSet() && !schema.getValue().contains("$")) {
			try (Connection connection = connect(context)) {
				SchemaManager schemaManager = connection.as(ReasoningConnection.class).getSchemaManager();
				Set<String> schemas = schemaManager.getSchemas();
				if (!schemas.contains(schema.getValue())) {
					String msg = String.format("Unrecognized schema: '%s'. Valid values: %s", schema, schemas);
					results.add(new ValidationResult.Builder().subject(REASONING_SCHEMA.getDisplayName())
					                                          .valid(false)
					                                          .explanation(msg)
					                                          .build());
				}
			}
			catch (RuntimeException e) {
				getLogger().debug("Error validating schema: ", e);
			}
		}
	}

	protected Connection connect(PropertyContext context) {
		ConnectionConfiguration configuration = getConnectionConfiguration(context);

		PropertyValue reasoningValue = context.getProperty(REASONING);
		if (reasoningValue.isSet() && !reasoningValue.getValue().contains("$")) {
			configuration.reasoning(reasoningValue.asBoolean());

			if (reasoningValue.asBoolean()) {
				PropertyValue schemaValue = context.getProperty(REASONING_SCHEMA);
				if (schemaValue.isSet() && !schemaValue.getValue().contains("$")) {
					configuration.schema(schemaValue.getValue());
				}
			}
		}

		return configuration.connect();
	}
}
