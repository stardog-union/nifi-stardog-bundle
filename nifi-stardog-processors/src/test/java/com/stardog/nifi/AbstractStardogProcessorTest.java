// Copyright (c) 2010 - 2020, Stardog Union. <http://www.stardog.com>
// For more information about licensing and copyright of this software, please contact
// sales@stardog.com or visit http://stardog.com

package com.stardog.nifi;

import java.util.Collection;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import static com.stardog.nifi.AbstractStardogProcessor.PASSWORD;
import static com.stardog.nifi.AbstractStardogProcessor.SERVER;
import static com.stardog.nifi.AbstractStardogProcessor.USERNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractStardogProcessorTest {

	protected abstract Class<? extends AbstractStardogProcessor> getProcessorClass();

	protected TestRunner newTestRunner() {
		TestRunner runner = TestRunners.newTestRunner(getProcessorClass());
		runner.setProperty(SERVER, "http://localhost:1234/foo");
		runner.setProperty(USERNAME, "username");
		runner.setProperty(PASSWORD, "password");
		return runner;
	}

	protected void assertSingleValidationResult(TestRunner runner, String message) {
		Collection<ValidationResult> results = null;
		if (runner.getProcessContext() instanceof MockProcessContext) {
			results = ((MockProcessContext) runner.getProcessContext()).validate();
		}
		assertNotNull(results);
		assertEquals("Expect a single result of " + message, 1, results.size());
		assertEquals(message, results.iterator().next().toString());
	}
}
