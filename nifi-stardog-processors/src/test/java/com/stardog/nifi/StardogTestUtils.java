package com.stardog.nifi;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.complexible.stardog.api.SelectQuery;
import com.stardog.stark.Value;
import com.stardog.stark.query.BindingSet;
import com.stardog.stark.query.QueryExecutionFailure;
import com.stardog.stark.query.SelectQueryResult;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class StardogTestUtils {

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

	public static void assertQueryResults(SelectQueryResult theResults, Value[]... expectedBindings) {
		assertQueryResults(theResults, null, expectedBindings);
	}

	/**
	 * For queries with multiple projected variables but only a single result.
	 */
	public static void assertQueryResult(SelectQuery theQuery, Value... expectedValues)  {
		assertQueryResults(theQuery.execute(), new Value[][] {expectedValues});
	}

	public static void assertQueryResults(SelectQueryResult actualQueryResult, String[] theBindingNames, Value[]... expectedBindings) {
		try {
			String[] aBindingNames = theBindingNames != null
			                         ? theBindingNames
			                         : actualQueryResult.variables().toArray(new String[0]);

			Multiset<Map<String, Value>> expected = createQueryResult(aBindingNames, expectedBindings);
			Multiset<Map<String, Value>> actual = convertQueryResult(actualQueryResult);

			assertEqualsUnordered(expected, actual);
		}
		finally {
			actualQueryResult.close();
		}
	}

	public static Multiset<Map<String,Value>> createQueryResult(String[] theBindingNames, Value[]... expectedBindings) throws QueryExecutionFailure {
		Multiset<Map<String,Value>> aResult = HashMultiset.create();
		for (Value[] expectedBinding : expectedBindings) {
			Map<String,Value> aMap = new LinkedHashMap<>();
			assertEquals("Number of variables in the query and expected query results do not match", expectedBinding.length, theBindingNames.length);
			for (int i = 0; i < expectedBinding.length; i++) {
				assertNotNull(theBindingNames[i]);
				aMap.put(theBindingNames[i], expectedBinding[i]);
			}
			aResult.add(aMap);
		}
		return aResult;
	}

	private static Map<String, Value> bindingSetToMap(List<String> theBindingNames, BindingSet theBindingSet) {
		Map<String,Value> aMap = new LinkedHashMap<>();
		for (String aBindingName : theBindingNames) {
			aMap.put(aBindingName, theBindingSet.value(aBindingName).orElse(null));
		}
		return aMap;
	}

	public static Multiset<Map<String,Value>> convertQueryResult(SelectQueryResult queryResult) throws QueryExecutionFailure {
		Multiset<Map<String,Value>> aResult = HashMultiset.create();
		queryResult.stream().map(b -> bindingSetToMap(queryResult.variables(), b)).forEach(aResult::add);
		return aResult;
	}
}
