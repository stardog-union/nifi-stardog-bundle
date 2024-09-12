// Copyright (c) 2010 - 2020, Stardog Union. <http://www.stardog.com>
// For more information about licensing and copyright of this software, please contact
// sales@stardog.com or visit http://stardog.com

package com.stardog.nifi;

import org.apache.nifi.util.TestRunner;
import org.junit.Test;

import static com.stardog.nifi.StardogPut.INPUT_FORMAT;
import static com.stardog.nifi.StardogPut.MAPPINGS_FILE;
import static com.stardog.nifi.StardogPut.UNIQUE_KEY_SETS;

public class StardogPutTest extends AbstractStardogProcessorTest {

	public static final String RESOURCES = "src/test/resources/";

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
		return RESOURCES + "mappings_file.sms";
	}
}
