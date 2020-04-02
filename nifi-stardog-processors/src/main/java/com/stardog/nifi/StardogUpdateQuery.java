/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stardog.nifi;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.complexible.common.rdf.query.SPARQLUtil;
import com.complexible.common.rdf.query.SPARQLUtil.QueryType;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.UpdateQuery;

import com.google.api.client.util.Sets;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "stardog", "sparql", "read" })
@CapabilityDescription("Execute provided SPARQL update query. This processor can be scheduled to run on a timer, or cron expression, using the standard " +
                       "scheduling methods, or it can be triggered by an incoming FlowFile. If it is triggered by an incoming FlowFile, then " +
                       "attributes of that FlowFile will be available when evaluating the query but the contents of that file will not be used. ")
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SeeAlso({})
public class StardogUpdateQuery extends AbstractStardogProcessor {

	public static final PropertyDescriptor QUERY =
			new PropertyDescriptor.Builder()
					.name("Query")
					.description("SPARQL update query")
					.required(true)
					.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
					.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
					.build();

	private static final List<PropertyDescriptor> PROPERTIES =
			ImmutableList.<PropertyDescriptor>builder()
					.addAll(DEFAULT_PROPERTIES)
					.add(QUERY)
					.build();

	@Override
	protected void init(final ProcessorInitializationContext context) {

	}

	@Override
	public Set<Relationship> getRelationships() {
		return DEFAULT_RELATIONSHIPS;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return PROPERTIES;
	}

	@Override
	protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
		Set<ValidationResult> results = Sets.newHashSet();

		String queryStr = validationContext.getProperty(QUERY).getValue();
		QueryType queryType = SPARQLUtil.getType(queryStr);

		if (queryType != QueryType.UPDATE) {
			String msg = String.format("Unsupported query type: %s", queryType);
			results.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
		}

		return results;
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile inputFile = getOptionalFlowFile(context, session);
		if (inputFile == null) {
			return;
		}

		Stopwatch stopwatch = Stopwatch.createStarted();

		ComponentLog logger = getLogger();

		String queryStr = context.getProperty(QUERY).evaluateAttributeExpressions(inputFile).getValue();

		try (Connection connection = connect(context)) {
			UpdateQuery query = connection.update(queryStr);

			query.execute();

			logger.info("Update completed; transferring {} to 'success'", new Object[] { inputFile });
			session.getProvenanceReporter()
			       .modifyContent(inputFile, "Executed update query", stopwatch.elapsed(TimeUnit.MILLISECONDS));
			session.transfer(inputFile, REL_SUCCESS);
		}
		catch (Throwable t) {
			Throwable rootCause = Throwables.getRootCause(t);
			context.yield();
			logger.error("{} failed! Throwable exception {}; rolling back session", new Object[] { this, rootCause });
			session.transfer(inputFile, REL_FAILURE);
		}
	}
}
