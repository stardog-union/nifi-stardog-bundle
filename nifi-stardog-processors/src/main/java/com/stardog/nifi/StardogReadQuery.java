// Copyright (c) 2010 - 2020, Stardog Union. <http://www.stardog.com>
// For more information about licensing and copyright of this software, please contact
// sales@stardog.com or visit http://stardog.com

package com.stardog.nifi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.complexible.common.base.Objects2;
import com.complexible.common.rdf.query.SPARQLUtil;
import com.complexible.common.rdf.query.SPARQLUtil.QueryType;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.GraphQuery;
import com.complexible.stardog.api.ReadQuery;
import com.complexible.stardog.api.SelectQuery;
import com.stardog.stark.io.FileFormat;
import com.stardog.stark.io.RDFFormat;
import com.stardog.stark.io.RDFFormats;
import com.stardog.stark.io.RDFWriter;
import com.stardog.stark.io.RDFWriters;
import com.stardog.stark.query.GraphQueryResult;
import com.stardog.stark.query.SelectQueryResult;
import com.stardog.stark.query.io.QueryResultFormat;
import com.stardog.stark.query.io.QueryResultFormats;
import com.stardog.stark.query.io.QueryResultWriters;
import com.stardog.stark.query.io.SelectQueryResultWriter;

import com.google.api.client.util.ByteStreams;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "stardog", "sparql", "read" })
@CapabilityDescription("Execute provided SPARQL read query (SELECT, CONSTRUCT, DESCRIBE) over a Stardog database. Streaming is used so arbitrarily " +
                       "large result sets are supported. This processor can be scheduled to run on a timer, or cron expression, using the standard " +
                       "scheduling methods, or it can be triggered by an incoming FlowFile.  If it is triggered by an incoming FlowFile, then " +
                       "attributes of that FlowFile will be available when evaluating the query but the contents of that file will not be used.")
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@WritesAttributes({ @WritesAttribute(attribute = "result.count", description = "The number of rows returned by the select query") })
public class StardogReadQuery extends AbstractStardogQueryProcessor {

	public static final String BYTE_COUNT = "byte.count";
	public static final String RESULT_COUNT = "result.count";

	public static final String DEFAULT_FORMAT = "CSV";

	private static Map<QueryType, FileFormat> formats(FileFormat... formats) {
		EnumMap<QueryType, FileFormat> map = new EnumMap<>(QueryType.class);
		for (FileFormat format : formats) {
			if (format instanceof QueryResultFormat) {
				map.put(QueryType.SELECT, format);
			}
			else if (format instanceof RDFFormat) {
				map.put(QueryType.GRAPH, format);
				map.put(QueryType.DESCRIBE, format);
			}
			else {
				throw new IllegalArgumentException("Invalid format: " + format);
			}
		}
		return ImmutableMap.copyOf(map);
	}

	public static final Map<String, Map<QueryType, FileFormat>> OUTPUT_FORMATS =
			ImmutableMap.<String, Map<QueryType, FileFormat>>builder()
					.put("CSV", formats(QueryResultFormats.CSV))
					.put("TSV", formats(QueryResultFormats.TSV))
					.put("XML", formats(QueryResultFormats.XML, RDFFormats.RDFXML))
					.put("JSON", formats(QueryResultFormats.JSON, RDFFormats.JSONLD))
					.put("Turtle", formats(RDFFormats.TURTLE))
					.put("TriG", formats(RDFFormats.TRIG))
					.put("N-Triples", formats(RDFFormats.NTRIPLES))
					.put("N-Quads", formats(RDFFormats.NQUADS))
					.build();

	public static final PropertyDescriptor QUERY_NAME =
			new PropertyDescriptor.Builder()
					.name("Query Name")
					.description("Stored SPARQL read query name")
					.required(false)
					.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.build();

	public static final PropertyDescriptor QUERY =
			new PropertyDescriptor.Builder()
					.name("Query")
					.description("SPARQL read query")
					.required(false)
					.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.build();

	public static final PropertyDescriptor OUTPUT_FORMAT =
			new PropertyDescriptor.Builder()
					.name("Output Format")
					.description("The format to which the result rows will be converted.")
					.required(true)
					.allowableValues(OUTPUT_FORMATS.keySet())
					.defaultValue(DEFAULT_FORMAT)
					.build();

	public static final PropertyDescriptor OUTPUT_ATTRIBUTE =
			new PropertyDescriptor.Builder()
					.name("Output Attribute")
					.description("Select the type of the output attribute. The output attribute will either be the number " +
					             "of bytes in the serialization of query results or the number of results returned by the " +
					             "query (slow). The number of results correspond to the number of rows for SELECT queries " +
					             "and number of triples for CONSTRUCT or DESCRIBE queries. Returning the number of results " +
					             "instead of bytes requires additional processing and can slow down generating large results."
					)
					.required(false)
					.allowableValues(new AllowableValue(BYTE_COUNT, "Byte count (FASTER)"),
							new AllowableValue(RESULT_COUNT,  "Result count (SLOWER)"))
					.defaultValue(BYTE_COUNT)
					.build();

	private static final List<PropertyDescriptor> PROPERTIES =
			ImmutableList.<PropertyDescriptor>builder()
					.addAll(DEFAULT_PROPERTIES)
					.add(QUERY_NAME)
					.add(QUERY)
					.add(QUERY_TIMEOUT)
					.add(OUTPUT_FORMAT)
					.add(OUTPUT_ATTRIBUTE)
					.add(REASONING)
					.add(REASONING_SCHEMA)
					.build();

	public StardogReadQuery() {
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
	protected void customValidate(ValidationContext context, Set<ValidationResult> results) {
		validateQueryName(context, results);

		String queryStr = context.getProperty(QUERY).getValue();
		if (queryStr != null && !queryStr.trim().startsWith("$")) {
			QueryType queryType = SPARQLUtil.getType(queryStr);

			if (queryType != QueryType.SELECT && queryType != QueryType.GRAPH) {
				String msg = String.format("Unsupported query type: %s", queryType);
				results.add(new ValidationResult.Builder().subject(QUERY.getDisplayName())
				                                          .valid(false)
				                                          .explanation(msg)
				                                          .build());
			}
			else {
				String selectedFormat = context.getProperty(OUTPUT_FORMAT).getValue();
				Map<QueryType, FileFormat> outputFormats = OUTPUT_FORMATS.get(selectedFormat);
				FileFormat outputFormat = outputFormats.get(queryType);

				if (outputFormat == null) {
					String msg = String.format("Query output format %s is not valid for given query type %s",
							selectedFormat, queryType);
					results.add(new ValidationResult.Builder().subject(OUTPUT_FORMAT.getDisplayName())
					                                          .input(selectedFormat)
					                                          .valid(false)
					                                          .explanation(msg)
					                                          .build());
				}
			}
		}

		validateSchema(context, results);
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile inputFile = getOptionalFlowFile(context, session);
		if (inputFile == null) {
			return;
		}

		Stopwatch stopwatch = Stopwatch.createStarted();

		FlowFile outputFile;

		ComponentLog logger = getLogger();

		try (Connection connection = connect(context, inputFile)) {
			long queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(inputFile).asTimePeriod(TimeUnit.MILLISECONDS);
			String queryStr = getQueryString(context, inputFile, connection);
			QueryType queryType = SPARQLUtil.getType(queryStr);
			String selectedFormat = context.getProperty(OUTPUT_FORMAT).getValue();
			Map<QueryType, FileFormat> outputFormats = OUTPUT_FORMATS.get(selectedFormat);
			FileFormat outputFormat = outputFormats.get(queryType);
			String outputAttribute = context.getProperty(OUTPUT_ATTRIBUTE).getValue();
			boolean isByteCount = outputAttribute.equals(BYTE_COUNT);

			MutableLong outpuAttributeValue = new MutableLong(0L);

			ReadQuery<?> query = (ReadQuery<?>) createQuery(connection, queryStr, queryType)
					.timeout(queryTimeout);

			getBindings(context, inputFile, connection).forEach(query::parameter);

			outputFile = session.write(inputFile, stream -> outpuAttributeValue.setValue(executeQuery(query, stream, outputFormat, isByteCount)));

			outputFile = session.putAttribute(outputFile, outputAttribute, outpuAttributeValue.toString());

			outputFile = session.putAttribute(outputFile, CoreAttributes.MIME_TYPE.key(), outputFormat.defaultMimeType());

			logger.info("{} contains {} results; transferring to 'success'", new Object[] { outputFile, outpuAttributeValue });
			session.getProvenanceReporter()
			       .modifyContent(outputFile, "Retrieved " + outpuAttributeValue + " results", stopwatch.elapsed(TimeUnit.MILLISECONDS));
			session.transfer(outputFile, REL_SUCCESS);
		}
		catch (Throwable t) {
			Throwable rootCause = Throwables.getRootCause(t);
			context.yield();
			logger.error("{} failed! Throwable exception {}; rolling back session", new Object[] { this, rootCause });
			session.transfer(inputFile, REL_FAILURE);
		}
	}

	private ReadQuery<?> createQuery(Connection connection, String queryStr, QueryType queryType) {
		switch (queryType) {
			case SELECT:
				return connection.select(queryStr);
			case GRAPH:
				return connection.graph(queryStr);
			default:
				throw new IllegalArgumentException("Unrecognized query type: " + queryType);
		}
	}

	private long executeQuery(ReadQuery<?> query, OutputStream out, FileFormat outputFormat, final boolean isByteCount) throws IOException {
		if (isByteCount) {
			try (InputStream in = query.execute(outputFormat)) {
				return ByteStreams.copy(in, out);
			}
		}
		else if (query instanceof SelectQuery) {
			return executeSelectQuery((SelectQuery) query, out, (QueryResultFormat) outputFormat);
		}
		else {
			return executeGraphQuery((GraphQuery) query, out, (RDFFormat) outputFormat);
		}
	}

	private long executeSelectQuery(SelectQuery query, OutputStream out, QueryResultFormat outputFormat) {
		SelectQueryResultWriter writer = Objects2.castTo(QueryResultWriters.to(out, outputFormat)
		                                                                   .orElseThrow(() -> new ProcessException("Unrecognized query result format " + outputFormat)),
		                                                 SelectQueryResultWriter.class,
		                                                 "Invalid select query result format: {}",
		                                                 outputFormat);

		long count = 0;
		try (SelectQueryResult result = query.execute()) {
			writer.start();
			writer.variables(result.variables());
			while (result.hasNext()) {
				writer.handle(result.next());
				count++;
			}
			writer.end();
		}
		return count;
	}

	private long executeGraphQuery(GraphQuery query, OutputStream out, RDFFormat outputFormat) {
		RDFWriter writer = RDFWriters.to(out, outputFormat)
		                             .orElseThrow(() -> new ProcessException("Unrecognized query result format " + outputFormat));

		long count = 0;
		try (GraphQueryResult result = query.execute()) {
			writer.start();
			result.namespaces()
			      .forEach(ns -> writer.namespace(ns.prefix(), ns.iri()));
			while (result.hasNext()) {
				writer.handle(result.next());
				count++;
			}
			writer.end();
		}
		return count;
	}
}
