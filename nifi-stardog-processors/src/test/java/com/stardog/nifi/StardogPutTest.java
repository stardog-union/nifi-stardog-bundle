package com.stardog.nifi;

import java.util.Objects;

import org.apache.nifi.util.TestRunner;
import org.junit.Test;

import static com.stardog.nifi.StardogPut.INPUT_FORMAT;
import static com.stardog.nifi.StardogPut.MAPPINGS_FILE;
import static com.stardog.nifi.StardogPut.UNIQUE_KEY_SETS;

public class StardogPutTest extends AbstractStardogProcessorTest {

	@Override
	protected Class<? extends AbstractStardogProcessor> getProcessorClass() {
		return StardogPut.class;
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

	private String getTestMappingFile() {
		return Objects.requireNonNull(this.getClass().getClassLoader().getResource("mappings_file.sms")).getPath();
	}
}
