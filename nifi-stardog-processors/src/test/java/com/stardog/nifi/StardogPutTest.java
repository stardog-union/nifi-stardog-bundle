package com.stardog.nifi;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import com.complexible.stardog.api.Connection;
import com.stardog.stark.Values;
import com.stardog.stark.vocabs.RDF;
import com.stardog.stark.io.RDFFormats;
import com.stardog.stark.Statement;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.TestRunner;
import org.junit.Before;
import org.junit.Test;

import static com.stardog.nifi.AbstractStardogProcessor.SERVER;
import static com.stardog.nifi.StardogPut.*;
import static com.stardog.nifi.StardogTestUtils.assertQueryResult;

public class StardogPutTest extends AbstractStardogProcessorTest {

	public static final String RESOURCES = "src/test/resources/";

	@Override
	protected Class<? extends AbstractStardogProcessor> getProcessorClass() {
		return StardogPut.class;
	}

	@Before
	public void clearAll() {
		initStardog();
	}

	@Test
	public void testCsvValidation() {
		TestRunner runner = newTestRunner();
		runner.setProperty(INPUT_FORMAT, "CSV");

		assertSingleValidationResult(runner,
				"'Unique Key Sets' is invalid because Unique Key Sets must be set when Mappings File is not set and Input Format is CSV");

		runner.setProperty(UNIQUE_KEY_SETS, "(x)");
		runner.assertValid();

		runner.removeProperty(UNIQUE_KEY_SETS);
		runner.assertNotValid();

		runner.setProperty(MAPPINGS_FILE, getTestMappingFile());
		runner.assertValid();
	}

	@Test
	public void testJsonValidation() {
		TestRunner runner = newTestRunner();
		runner.setProperty(INPUT_FORMAT, "JSON");

		assertSingleValidationResult(runner,
				"'Mappings File' is invalid because Mappings File must be set when Input Format is JSON");

		runner.setProperty(MAPPINGS_FILE, getTestMappingFile());
		runner.assertValid();
	}

	@Test
	public void testSetServerViaVariable() {
		TestRunner runner = newTestRunner();

		runner.setVariable(DATABASE_VAR_NAME, getStardogDatabase());
		runner.enqueue("{ \"val\" : \"1\" }");

		runServerExpressionTest(runner);
	}

	@Test
	public void testSetServerViaAttribute() {
		TestRunner runner = newTestRunner();

		Map<String, String> attributes = Collections.singletonMap(DATABASE_VAR_NAME, getStardogDatabase());
		runner.enqueue("{ \"val\" : \"1\" }", attributes);

		runServerExpressionTest(runner);
	}

	@Test
	public void testClearQuery() {
		// Obsolete statement to be replaced
		Statement obsolete = Values.statement(Values.iri(NS, "1"),
				RDF.TYPE,
				Values.iri(NS, "OldType"),
				Values.iri("tag:g1"));

		try (Connection connection = connect()) {
			connection.namespaces().add("",NS);
			connection.begin();
			connection.add().statement(obsolete);
			connection.commit();
			assertQueryResult(connection.select("select * { graph <tag:g1> { ?s ?p ?o } }"),
					obsolete.subject(),
					obsolete.predicate(),
					obsolete.object());
		}
		// Assign a SPARQL update to selectively clear the graph before the ingestion
		TestRunner runner = newTestRunner();
		runner.setEnvironmentVariableValue(DATABASE_VAR_NAME, getStardogDatabase());
		runner.enqueue("{ \"val\" : \"1\" }");
		runServerExpressionTest(runner, "prefix : <"+ NS +"> with <tag:g1> delete { ?s a :OldType } where { ?s a :OldType }");
	}

	private void runServerExpressionTest(TestRunner runner, String query) {
		if(query == null){
			runner.setProperty(CLEAR_TARGET_GRAPH, "true");
		}
		else{
			runner.setProperty(QUERY, query);
		}
		runServerExpressionTest(runner);
	}

	private void runServerExpressionTest(TestRunner runner) {
		runner.setProperty(INPUT_FORMAT, "JSON");
		runner.setProperty(TARGET_GRAPH, "tag:g1");
		runner.setProperty(MAPPINGS_FILE, getTestMappingFile());
		runner.setProperty(SERVER, connectionStringWithDbExpression());

		runner.assertValid();

		runner.run();

		assertLogMessagesSize(0, runner.getLogger().getErrorMessages());

		runner.assertTransferCount(AbstractStardogProcessor.REL_SUCCESS, 1);

		try (Connection connection = connect()) {
			assertQueryResult(connection.select("select * { graph <tag:g1> { ?s ?p ?o } }"),
					Values.iri(NS, "1"),
					RDF.TYPE,
					Values.iri(NS, "Widget"));
		}
	}

	private String getTestMappingFile() {
		return RESOURCES + "mappings_file.sms";
	}
}
