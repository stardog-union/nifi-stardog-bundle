// Copyright (c) 2010 - 2020, Stardog Union. <http://www.stardog.com>
// For more information about licensing and copyright of this software, please contact
// sales@stardog.com or visit http://stardog.com

package com.stardog.nifi;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionConfiguration;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.hamcrest.Matchers;
import org.junit.Assume;

import static com.stardog.nifi.AbstractStardogProcessor.SERVER;
import static com.stardog.nifi.StardogClientService.PASSWORD;
import static com.stardog.nifi.StardogClientService.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public abstract class AbstractStardogProcessorTest {

	protected static final String NIFI_STARDOG_ENDPOINT_ENV = "NIFI_STARDOG_ENDPOINT";

	protected static final String NIFI_STARDOG_USERNAME_ENV = "NIFI_STARDOG_USERNAME";

	protected static final String NIFI_STARDOG_PASSWORD_ENV = "NIFI_STARDOG_PASSWORD";

	// e.g. "http://localhost:5820/nifi" - a db with reasoning.schema.graphs "urn:g1,urn:g2" and reasoning.schema "g1=urn:g1,g2=urn:g2"
	protected static final String STARDOG_ENDPOINT = System.getenv(NIFI_STARDOG_ENDPOINT_ENV);

	protected static final String STARDOG_USERNAME = getEnvWithDefault(NIFI_STARDOG_USERNAME_ENV, "admin");

	protected static final String STARDOG_PASSWORD = getEnvWithDefault(NIFI_STARDOG_PASSWORD_ENV, "admin");

	protected abstract Class<? extends AbstractStardogProcessor> getProcessorClass();

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

	/**
	 * Asserts that the iterator for the given iterable returns elements of the given array in some order
	 */
	public static <T> void assertEqualsUnordered(final Iterable<T> theActual, final T... theExpected) {
		assertEqualsUnordered(Arrays.asList(theExpected), theActual);
	}

	/**
	 * Asserts that the iterator for the given iterable returns elements of the other iterable in some order
	 */
	public static void assertEqualsUnordered(final Iterable<?> theExpected, final Iterable<?> theActual) {
		assertEqualsUnordered("", theExpected, theActual);
	}

	/**
	 * Asserts that the iterator for the given iterable returns elements of the other iterable in some order
	 */
	public static void assertEqualsUnordered(String msg, final Iterable<?> theExpected, final Iterable<?> theActual) {
		Multiset<Object> missing = HashMultiset.create(theExpected);
		int theExpectedCount = missing.size();
		int theActualCount = 0;
		Set<Object> unexpected = Sets.newHashSet();
		for (Object obj : theActual) {
			theActualCount++;
			if (!missing.remove(obj)) {
				unexpected.add(obj);
			}
		}
		StringBuilder failureMsg = new StringBuilder(msg).append(" ");
		// Sometimes the actuals are expected but fails because there are duplicates, which was confusing
		int difference = theActualCount - theExpectedCount;
		int distinctDifference = unexpected.size() - missing.size();
		if (difference != 0 && difference != distinctDifference) {
			failureMsg.append("\n")
			          .append(Math.abs(difference))
			          .append(difference > 0 ? " more" : " fewer")
			          .append(" records than expected.");
		}
		if (!unexpected.isEmpty()) {
			failureMsg.append("\nUnexpected (").append(unexpected.size()).append("):\n");
			Joiner.on("\n").appendTo(failureMsg, Iterables.limit(unexpected, 10));
			if (unexpected.size() > 10) {
				failureMsg.append("\n[").append(unexpected.size() - 10).append(" more]");
			}
		}
		if (!missing.isEmpty()) {
			failureMsg.append("\nMissing (").append(missing.size()).append("):\n");
			Joiner.on("\n").appendTo(failureMsg, Iterables.limit(missing, 10));
			if (missing.size() > 10) {
				failureMsg.append("\n[").append(missing.size() - 10).append(" more]");
			}
		}
		if (!missing.isEmpty() || !unexpected.isEmpty()) {
			System.err.println("Contents differ: " + failureMsg);
			fail(failureMsg.toString());
		}
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
}
