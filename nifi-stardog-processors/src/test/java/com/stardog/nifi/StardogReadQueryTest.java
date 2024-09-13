// Copyright (c) 2010 - 2020, Stardog Union. <http://www.stardog.com>
// For more information about licensing and copyright of this software, please contact
// sales@stardog.com or visit http://stardog.com

package com.stardog.nifi;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.io.CharStreams;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.stardog.nifi.AbstractStardogProcessor.SERVER;
import static com.stardog.nifi.AbstractStardogQueryProcessor.REASONING;
import static com.stardog.nifi.AbstractStardogQueryProcessor.REASONING_SCHEMA;
import static com.stardog.nifi.StardogReadQuery.OUTPUT_ATTRIBUTE;
import static com.stardog.nifi.StardogReadQuery.OUTPUT_FORMAT;
import static com.stardog.nifi.StardogReadQuery.QUERY;
import static com.stardog.nifi.StardogReadQuery.QUERY_NAME;
import static com.stardog.nifi.StardogTestUtils.assertEqualsUnordered;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StardogReadQueryTest extends AbstractStardogQueryTest {

	public StardogReadQueryTest() {
		super(QUERY_NAME, QUERY);
	}

	@BeforeClass
	public static void beforeClass() {
		initStardog();
	}

	@Override
	protected Class<? extends AbstractStardogProcessor> getProcessorClass() {
		return StardogReadQuery.class;
	}

	@Override
	protected String getValidQuery() {
		return "SELECT * { ?s ?p ?o }";
	}

	@Test
	public void testQueryTypeValidation() {
		TestRunner runner = newTestRunner();

		runner.setProperty(QUERY, "ASK { ?s ?p ?o }");
		assertSingleValidationResult(runner,
				"'Query' is invalid because Unsupported query type: ASK");

		runner.setProperty(QUERY, "PATHS START ?x = :Alice END ?y VIA ?p");
		assertSingleValidationResult(runner,
				"'Query' is invalid because Unsupported query type: PATH");

		runner.setProperty(QUERY, "INSERT DATA { :Dad :says :No }");
		assertSingleValidationResult(runner,
				"'Query' is invalid because Unsupported query type: UPDATE");

		runner.setProperty(QUERY, "CLEAR ALL");
		assertSingleValidationResult(runner,
				"'Query' is invalid because Unsupported query type: UPDATE");

		runner.setProperty(QUERY, "PREFIX : <http://stardog.com/nifi/>\n" +
		                          "CONSTRUCT   { :Alice :name ?name }\n" +
		                          "WHERE       { ?x :hasName ?name }");
		assertSingleValidationResult(runner,
				"'Output Format' validated against 'CSV' is invalid because " +
				"Query output format CSV is not valid for given query type GRAPH");

		runner.setProperty(OUTPUT_FORMAT, "Turtle");
		runner.assertValid();

		runner.setProperty(QUERY, "DESCRIBE :Me");
		runner.assertValid();
	}

	@Test
	public void testOutputAttribute() {
		assumeStardogAvailable();

		String[] expectedResults = {
				"s,p,o",
				"urn:test,urn:p,some value",
				"urn:test,urn:p,some other value",
				"urn:test2,urn:p2,urn:test",
				"urn:test3,urn:p,new value",
				"urn:test3,urn:p3,urn:test"
		};

		TestRunner runner = newTestRunner();

		runner.setProperty(QUERY, getValidQuery());
		runner.enqueue("");
		runner.run();

		MockFlowFile file = assertQuerySuccess(runner, expectedResults);
		file.assertAttributeEquals(StardogReadQuery.BYTE_COUNT, String.valueOf(file.toByteArray().length));
		file.assertAttributeNotExists(StardogReadQuery.RESULT_COUNT);

		runner.clearTransferState();
		runner.setProperty(OUTPUT_ATTRIBUTE, StardogReadQuery.RESULT_COUNT);
		runner.enqueue("");
		runner.run();

		file = assertQuerySuccess(runner, expectedResults);
		file.assertAttributeEquals(StardogReadQuery.RESULT_COUNT, "5");
		file.assertAttributeNotExists(StardogReadQuery.BYTE_COUNT);
	}

	private MockFlowFile assertQuerySuccess(TestRunner runner, String[] expectedResults) {
		runner.assertAllFlowFilesTransferred(AbstractStardogProcessor.REL_SUCCESS, 1);

		List<MockFlowFile> files = runner.getFlowFilesForRelationship(AbstractStardogProcessor.REL_SUCCESS);
		assertNotNull(files);
		assertEquals("One file should be transferred to success", 1, files.size());

		MockFlowFile file = files.get(0);

		if (expectedResults != null) {
			try {
				assertEqualsUnordered(CharStreams.readLines(new StringReader(new String(file.toByteArray()))), expectedResults);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		return file;
	}

	@Test
	public void testReasoningSchema() {
		assumeStardogAvailable();

		TestRunner runner = newTestRunner();

		runner.setProperty(QUERY, "SELECT * { ?s <urn:p> ?o }");
		runner.setProperty(OUTPUT_ATTRIBUTE, StardogReadQuery.RESULT_COUNT);
		runner.enqueue("");
		runner.run();

		// there are three asserted values
		assertQuerySuccess(runner, null)
				.assertAttributeEquals(StardogReadQuery.RESULT_COUNT, "3");

		runner.clearTransferState();
		runner.setProperty(REASONING, "true");
		runner.enqueue("");
		runner.run();

		// default reasoning schema contains two subproperties so we expect two inferences
		assertQuerySuccess(runner, null)
				.assertAttributeEquals(StardogReadQuery.RESULT_COUNT, "5");

		runner.clearTransferState();
		runner.setProperty(REASONING_SCHEMA, "g1");
		runner.enqueue("");
		runner.run();

		// g1 schema contains only one subproperty so we expect only one inferred result
		assertQuerySuccess(runner, null)
				.assertAttributeEquals(StardogReadQuery.RESULT_COUNT, "4");

	}

	@Test
	public void testReasoningSchemaConnectionUrlViaFlowFile() {
		TestRunner runner = newTestRunner();
		runner.setProperty(QUERY, "SELECT * { ?s <urn:p> ?o }");
		runner.setProperty(REASONING, "true");
		runner.setProperty(OUTPUT_ATTRIBUTE, StardogReadQuery.RESULT_COUNT);

		// Set the connection string to one that requires a var for the schema validation to work
		runner.setProperty(SERVER, connectionStringWithDbExpression());

		Map<String, String> attributes = Collections.singletonMap(DATABASE_VAR_NAME, getStardogDatabase());
		runner.enqueue("", attributes);

		runner.setProperty(REASONING_SCHEMA, "g1");

		runner.run();

		// g1 schema contains only one subproperty so we expect only one inferred result
		assertQuerySuccess(runner, null)
				.assertAttributeEquals(StardogReadQuery.RESULT_COUNT, "4");

		// Should have no errors and one debug for inability to validate schema given connection requires flowfile
		assertLogMessagesSize(1, runner.getLogger().getDebugMessages());
		assertLogMessagesSize(0, runner.getLogger().getErrorMessages());


		runner.clearTransferState();
		runner.setProperty(REASONING_SCHEMA, "bogus");
		runner.enqueue("", attributes);
		runner.run();

		// Should have an error for the invalid schema
		List<LogMessage> errorMessages = runner.getLogger().getErrorMessages();
		assertLogMessagesSize(1, errorMessages);
		MatcherAssert.assertThat(logMessageToString(errorMessages.get(0)), containsString("bogus"));

		List<MockFlowFile> files = runner.getFlowFilesForRelationship(AbstractStardogProcessor.REL_SUCCESS);
		assertNotNull(files);
		assertEquals("No files should be transferred to success", 0, files.size());

		files = runner.getFlowFilesForRelationship(AbstractStardogProcessor.REL_FAILURE);
		assertNotNull(files);
		assertEquals("One file should be transferred to failure", 1, files.size());
	}

	@Test
	public void testSchemaValidationConstantConnectionUrl() {
		TestRunner runner = newTestRunner();
		runner.setProperty(QUERY, "SELECT * { ?s <urn:p> ?o }");
		runner.setProperty(REASONING, "true");

		runner.setProperty(REASONING_SCHEMA, "g1");
		runner.assertValid();
		assertLogMessagesSize(0, runner.getLogger().getDebugMessages());

		runner.setProperty(REASONING_SCHEMA, "bogus");
		runner.assertNotValid();
		assertLogMessagesSize(0, runner.getLogger().getDebugMessages());
	}

	@Test
	public void testSchemaValidationUnsetVarInConnectionUrl() {
		TestRunner runner = newTestRunner();
		runner.setProperty(QUERY, "SELECT * { ?s <urn:p> ?o }");
		runner.setProperty(REASONING, "true");

		// Set the connection string to one that requires a var for the schema validation to work
		runner.setProperty(SERVER, connectionStringWithDbExpression());

		runner.setProperty(REASONING_SCHEMA, "g1");
		runner.assertValid();
		assertLogMessagesSize(1, runner.getLogger().getDebugMessages());
	}

	@Test
	public void testSchemaValidationConnectionUrlWithVar() {
		TestRunner runner = newTestRunner();
		runner.setProperty(QUERY, "SELECT * { ?s <urn:p> ?o }");
		runner.setProperty(REASONING, "true");

		// Set the connection string to one that requires a var for the schema validation to work
		runner.setProperty(SERVER, connectionStringWithDbExpression());

		// Set the DB var via variable
		runner.setVariable(DATABASE_VAR_NAME, getStardogDatabase());

		runner.setProperty(REASONING_SCHEMA, "g1");
		runner.assertValid();
		assertLogMessagesSize(0, runner.getLogger().getDebugMessages());

		runner.setProperty(REASONING_SCHEMA, "bogus");
		runner.assertNotValid();
		assertLogMessagesSize(0, runner.getLogger().getDebugMessages());
	}
}
