package com.stardog.nifi;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.TestRunner;
import org.junit.Test;

public abstract class AbstractStardogQueryTest extends AbstractStardogProcessorTest {

	private final PropertyDescriptor mQueryNameDescriptor;

	private final PropertyDescriptor mQueryDescriptor;

	public AbstractStardogQueryTest(PropertyDescriptor queryNameDescriptor, PropertyDescriptor queryDescriptor) {
		mQueryNameDescriptor = queryNameDescriptor;
		mQueryDescriptor = queryDescriptor;
	}

	protected abstract String getValidQuery();

	@Test
	public void testQueryNameValidation() {
		TestRunner runner = newTestRunner();

		assertSingleValidationResult(runner,
				"'Query' is invalid because Must set either Query Name or Query");

		runner.setProperty(mQueryNameDescriptor, "reademnweep");
		runner.setProperty(mQueryDescriptor, getValidQuery());
		assertSingleValidationResult(runner,
				"'Query' validated against 'reademnweep' is invalid because Cannot set both Query Name and Query");

		runner.removeProperty(mQueryDescriptor);
		runner.assertValid();

		runner.removeProperty(mQueryNameDescriptor);
		runner.setProperty(mQueryDescriptor, getValidQuery());
		runner.assertValid();
	}
}
