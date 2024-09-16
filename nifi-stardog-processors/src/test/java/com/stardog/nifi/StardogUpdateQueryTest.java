package com.stardog.nifi;

import java.util.Collections;
import java.util.Map;

import com.complexible.stardog.api.Connection;
import com.stardog.stark.Values;

import org.apache.nifi.util.TestRunner;
import org.junit.Before;
import org.junit.Test;

import static com.stardog.nifi.StardogTestUtils.assertQueryResult;
import static com.stardog.nifi.StardogUpdateQuery.QUERY;
import static com.stardog.nifi.StardogUpdateQuery.QUERY_NAME;

public class StardogUpdateQueryTest extends AbstractStardogQueryTest {

	public static final String API = "http://api.stardog.com/";

	public StardogUpdateQueryTest() {
		super(QUERY_NAME, QUERY);
	}

	@Before
	public void clearAll() {
		try (Connection connection = connect()) {
			connection.update("clear all").execute();
		}
	}

	@Override
	protected Class<? extends AbstractStardogProcessor> getProcessorClass() {
		return StardogUpdateQuery.class;
	}

	@Override
	protected String getValidQuery() {
		return "INSERT DATA { :I :am :U }";
	}

	@Test
	public void testQueryTypeValidation() {
		TestRunner runner = newTestRunner();

		runner.setProperty(QUERY, "SELECT * { ?s ?p ?o }");
		assertSingleValidationResult(runner,
				"'Query' is invalid because Unsupported query type: SELECT");

		runner.setProperty(QUERY, "ASK { ?s ?p ?o }");
		assertSingleValidationResult(runner,
				"'Query' is invalid because Unsupported query type: ASK");

		runner.setProperty(QUERY, "PATHS START ?x = :Alice END ?y VIA ?p");
		assertSingleValidationResult(runner,
				"'Query' is invalid because Unsupported query type: PATH");

		runner.setProperty(QUERY, "PREFIX : <http://stardog.com/nifi/>\n" +
		                          "CONSTRUCT   { :Alice :name ?name }\n" +
		                          "WHERE       { ?x :hasName ?name }");
		assertSingleValidationResult(runner,
				"'Query' is invalid because Unsupported query type: GRAPH");

		runner.setProperty(QUERY, "DESCRIBE :Me");
		assertSingleValidationResult(runner,
				"'Query' is invalid because Unsupported query type: GRAPH");

		runner.setProperty(QUERY, "INSERT DATA { :Dad :says :No }");
		runner.assertValid();

		runner.setProperty(QUERY, "CLEAR ALL");
		runner.assertValid();
	}

	@Test
	public void testRunUpdate() {
		TestRunner runner = newTestRunner();
		runner.enqueue("");

		runUpdateTest(runner);
	}

	@Test
	public void testSetServerViaVariable() {
		TestRunner runner = newTestRunner();

		runner.setVariable(DATABASE_VAR_NAME, getStardogDatabase());
		runner.enqueue("");

		runUpdateTest(runner);
	}

	@Test
	public void testSetServerViaAttribute() {
		TestRunner runner = newTestRunner();

		Map<String, String> attributes = Collections.singletonMap(DATABASE_VAR_NAME, getStardogDatabase());
		runner.enqueue("", attributes);

		runUpdateTest(runner);
	}

	private void runUpdateTest(TestRunner runner) {
		runner.setProperty(QUERY, getValidQuery());
		runner.assertValid();

		runner.run();

		assertLogMessagesSize(0, runner.getLogger().getErrorMessages());

		runner.assertTransferCount(AbstractStardogProcessor.REL_SUCCESS, 1);

		try (Connection connection = connect()) {
			assertQueryResult(connection.select("select * { ?s ?p ?o }"),
					Values.iri(API, "I"),
					Values.iri(API, "am"),
					Values.iri(API, "U"));
		}
	}
}
