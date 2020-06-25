package com.stardog.nifi;

import org.apache.nifi.util.TestRunner;
import org.junit.Test;

import static com.stardog.nifi.StardogReadQuery.OUTPUT_FORMAT;
import static com.stardog.nifi.StardogReadQuery.QUERY;
import static com.stardog.nifi.StardogReadQuery.QUERY_NAME;

public class StardogReadQueryTest extends AbstractStardogQueryTest {

	public StardogReadQueryTest() {
		super(QUERY_NAME, QUERY);
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
}
