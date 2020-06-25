package com.stardog.nifi;

import org.apache.nifi.util.TestRunner;
import org.junit.Test;

import static com.stardog.nifi.StardogUpdateQuery.QUERY;
import static com.stardog.nifi.StardogUpdateQuery.QUERY_NAME;

public class StardogUpdateQueryTest extends AbstractStardogQueryTest {

	public StardogUpdateQueryTest() {
		super(QUERY_NAME, QUERY);
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
}
