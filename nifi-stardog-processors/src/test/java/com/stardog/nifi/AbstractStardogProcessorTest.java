package com.stardog.nifi;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionConfiguration;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.hamcrest.Matchers;
import org.junit.Assume;
import org.slf4j.helpers.MessageFormatter;

import static com.stardog.nifi.AbstractStardogProcessor.SERVER;
import static com.stardog.nifi.StardogClientService.PASSWORD;
import static com.stardog.nifi.StardogClientService.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractStardogProcessorTest {

	public final static String NS = "http://example.com/";

	protected static final String NIFI_STARDOG_ENDPOINT_ENV = "NIFI_STARDOG_ENDPOINT";

	protected static final String NIFI_STARDOG_USERNAME_ENV = "NIFI_STARDOG_USERNAME";

	protected static final String NIFI_STARDOG_PASSWORD_ENV = "NIFI_STARDOG_PASSWORD";

	// e.g. "http://localhost:5820/nifi" - a db with reasoning.schema.graphs "urn:g1,urn:g2" and reasoning.schema "g1=urn:g1,g2=urn:g2"
	protected static final String STARDOG_ENDPOINT = System.getenv(NIFI_STARDOG_ENDPOINT_ENV);

	protected static final String STARDOG_USERNAME = getEnvWithDefault(NIFI_STARDOG_USERNAME_ENV, "admin");

	protected static final String STARDOG_PASSWORD = getEnvWithDefault(NIFI_STARDOG_PASSWORD_ENV, "admin");

	public static final String DATABASE_VAR_NAME = "stardog.database";

	protected static void assumeStardogAvailable() {
		Assume.assumeTrue("No Stardog endpoint available. Set " + NIFI_STARDOG_ENDPOINT_ENV +
		                  " environment variable to enable tests against a Stardog endpoint.",
				isStardogAvailable());
	}

	protected static boolean isStardogAvailable() {
		return STARDOG_ENDPOINT != null;
	}

	protected static String getStardogEndpoint() {
		return STARDOG_ENDPOINT == null
		       ? "http://localhost:5820/nifi"
		       : STARDOG_ENDPOINT;
	}

	protected static String getStardogUsername() {
		return STARDOG_USERNAME;
	}

	protected static String getStardogPassword() {
		return STARDOG_PASSWORD;
	}

	private static String getStardogAddressWithPort() {
		String endpoint = getStardogEndpoint();
		return endpoint.substring(0, endpoint.lastIndexOf('/') + 1);
	}

	protected static String getStardogDatabase() {
		String endpoint = getStardogEndpoint();
		return endpoint.substring(endpoint.lastIndexOf('/') + 1);
	}

	protected static void initStardog() {
		if (isStardogAvailable()) {
			try (Connection connection = ConnectionConfiguration.from(STARDOG_ENDPOINT)
			                                                    .credentials(STARDOG_USERNAME, STARDOG_PASSWORD)
			                                                    .connect()) {
				connection.update("CLEAR ALL").execute();

				connection.begin();
				connection.add()
				          .io()
				          .file(Paths.get("src/test/resources/add_data.nt"))
				          .file(Paths.get("src/test/resources/add_data_subprop1.nq"))
				          .file(Paths.get("src/test/resources/add_data_subprop2.nq"));
				connection.commit();
			}
		}
	}

	private static String getEnvWithDefault(String envName, String defaultValue) {
		String value = System.getenv(envName);
		return value == null
		       ? defaultValue
		       : value;
	}

	public static void assertLogMessagesSize(int expectedSize, List<LogMessage> logMessages) {
		assertEquals("Unexpected List<LogMessage> size:\n" +
		             logMessages.stream()
		                        .map(AbstractStardogProcessorTest::logMessageToString)
		                        .collect(Collectors.joining("\n")),
				expectedSize,
				logMessages.size());
	}

	public static String logMessageToString(LogMessage m) {
		return MessageFormatter.format(m.getMsg(), m.getArgs()).getMessage();
	}

	protected static String connectionStringWithDbExpression() {
		return getStardogAddressWithPort() + "${" + DATABASE_VAR_NAME + "}";
	}

	protected abstract Class<? extends AbstractStardogProcessor> getProcessorClass();

	protected TestRunner newTestRunner() {
		TestRunner runner = TestRunners.newTestRunner(getProcessorClass());
		runner.setProperty(SERVER, getStardogEndpoint());
		runner.setProperty(USERNAME, getStardogUsername());
		runner.setProperty(PASSWORD, getStardogPassword());
		return runner;
	}

	protected void assertSingleValidationResult(TestRunner runner, String message) {
		Collection<ValidationResult> results = null;
		if (runner.getProcessContext() instanceof MockProcessContext) {
			results = ((MockProcessContext) runner.getProcessContext()).validate();
		}
		assertNotNull(results);
		assertEquals("Expect a single result of: " + message + "\nActual: " + results, 1, results.size());
		assertEquals(message, results.iterator().next().toString());
	}

	protected void assertValidationResults(TestRunner runner, String... messages) {
		Collection<ValidationResult> results = null;
		if (runner.getProcessContext() instanceof MockProcessContext) {
			results = ((MockProcessContext) runner.getProcessContext()).validate();
		}
		assertNotNull(results);
		assertEquals("Expect " + messages.length + " results: " + Arrays.deepToString(messages), messages.length,
				results.size());
		results.stream()
		       .map(ValidationResult::toString)
		       .forEach(m -> assertThat(m, Matchers.in(messages)));
	}

	protected Connection connect() {
		return ConnectionConfiguration.to(getStardogDatabase())
		                              .server(getStardogAddressWithPort())
		                              .credentials(getStardogUsername(), getStardogPassword())
		                              .connect();
	}
}
