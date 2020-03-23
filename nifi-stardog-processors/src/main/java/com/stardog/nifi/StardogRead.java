/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stardog.nifi;

import java.io.OutputStream;
import java.util.Collection;
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
import com.complexible.stardog.api.Query;
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

import com.google.api.client.util.Sets;
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
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
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
                       "attributes of that FlowFile will be available when evaluating the query but the contents of that file will not be used. " +
                       "FlowFile attribute 'result.count' indicates how  many results are returned. This is the number of rows for SELECT queries " +
                       "and number of triples for CONSTRUCT or DESCRIBE queries.")
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SeeAlso({})
@WritesAttributes({ @WritesAttribute(attribute = "result.count", description = "The number of rows returned by the select query") })
public class StardogRead extends AbstractStardogProcessor {

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

	public static final PropertyDescriptor QUERY =
			new PropertyDescriptor.Builder()
					.name("Query")
					.description("SPARQL read query")
					.required(true)
					.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.build();

	public static final PropertyDescriptor QUERY_TIMEOUT =
			new PropertyDescriptor.Builder()
					.name("Query Timeout")
					.description("The maximum amount of time allowed for a running query. Must be of format "
					             + "<duration> <TimeUnit> where <duration> is a non-negative integer and TimeUnit is a supported "
					             + "Time Unit, such as: nanos, millis, secs, mins, hrs, days. A value of zero means there is no limit. ")
					.defaultValue("0 seconds")
					.required(true)
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
					.build();


	public static final PropertyDescriptor OUTPUT_FORMAT =
			new PropertyDescriptor.Builder()
					.name("Output Format")
					.description("The format to which the result rows will be converted.")
					.required(true)
					.allowableValues(OUTPUT_FORMATS.keySet())
					.defaultValue(DEFAULT_FORMAT)
					.build();

	private static final List<PropertyDescriptor> PROPERTIES =
			ImmutableList.<PropertyDescriptor>builder()
					.addAll(DEFAULT_PROPERTIES)
					.add(QUERY).add(QUERY_TIMEOUT).add(OUTPUT_FORMAT).build();

	@Override
	protected void init(final ProcessorInitializationContext context) {

	}

	@Override
	public Set<Relationship> getRelationships() {
		return DEFAULT_RELATIONSHIPS;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return PROPERTIES;
	}

	@Override
	protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
		Set<ValidationResult> results = Sets.newHashSet();

		String queryStr = validationContext.getProperty(QUERY).getValue();
		QueryType queryType = SPARQLUtil.getType(queryStr);

		if (queryType != QueryType.SELECT || queryType != QueryType.GRAPH) {
			String msg = String.format("Unsupported query type: %s", queryType);
			results.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
		}
		else {
			String selectedFormat = validationContext.getProperty(OUTPUT_FORMAT).getValue();
			Map<QueryType, FileFormat> outputFormats = OUTPUT_FORMATS.get(selectedFormat);
			FileFormat outputFormat = outputFormats.get(queryType);

			if (outputFormat == null) {
				String msg = String.format("Query output format %s is not valid for given query type %s", selectedFormat, queryType);
				results.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
			}
		}

		return results;
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile inputFile = null;
		if (context.hasIncomingConnection()) {
			inputFile = session.get();
		}

		if (inputFile == null) {
			inputFile = session.create();
		}

		Stopwatch stopwatch = Stopwatch.createStarted();

		FlowFile outputFile = null;

		ComponentLog logger = getLogger();

		long queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(inputFile).asTimePeriod(TimeUnit.MILLISECONDS);
		String queryStr = context.getProperty(QUERY).evaluateAttributeExpressions(inputFile).getValue();
		QueryType queryType = SPARQLUtil.getType(queryStr);
		String selectedFormat = context.getProperty(OUTPUT_FORMAT).getValue();
		Map<QueryType, FileFormat> outputFormats = OUTPUT_FORMATS.get(selectedFormat);
		FileFormat outputFormat = outputFormats.get(queryType);

		MutableLong resultCount = new MutableLong(0L);

		try (Connection connection = connect(context)) {
			Query<?> query = createQuery(connection, queryStr, queryType);

			// TODO set reasoning, dataset, parameters

			outputFile = session.write(inputFile, stream -> {
				resultCount.setValue(executeQuery(query, stream, outputFormat));
			});

			outputFile = session.putAttribute(outputFile, RESULT_COUNT, resultCount.toString());

			outputFile = session.putAttribute(outputFile, CoreAttributes.MIME_TYPE.key(), outputFormat.defaultMimeType());

			logger.info("{} contains {} results; transferring to 'success'", new Object[] { outputFile, resultCount });
			session.getProvenanceReporter()
			       .modifyContent(outputFile, "Retrieved " + resultCount + " results", stopwatch.elapsed(TimeUnit.MILLISECONDS));
			session.transfer(outputFile, REL_SUCCESS);
		}
		catch (Throwable t) {
			Throwable rootCause = Throwables.getRootCause(t);
			context.yield();
			logger.error("{} failed! Throwable exception {}; rolling back session", new Object[] { this, rootCause });
			session.rollback(true);
			session.transfer(outputFile, REL_FAILURE);
		}
	}

	private Query<?> createQuery(Connection connection, String queryStr, QueryType queryType) {
		// TODO support stored queries
		switch (queryType) {
			case SELECT:
				return connection.select(queryStr);
			case GRAPH:
				return connection.graph(queryStr);
			default:
				throw new IllegalArgumentException("Unrecognized query type: " + queryType);
		}
	}

	private long executeQuery(Query<?> query, OutputStream out, FileFormat outputFormat) {
		if (query instanceof SelectQuery) {
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
