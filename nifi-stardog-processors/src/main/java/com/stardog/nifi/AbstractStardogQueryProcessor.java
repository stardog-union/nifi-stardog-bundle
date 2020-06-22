package com.stardog.nifi;

import java.util.Set;

import com.complexible.stardog.api.Connection;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;

public abstract class AbstractStardogQueryProcessor extends AbstractStardogProcessor {

	private final PropertyDescriptor mQueryNameDescriptor;

	private final PropertyDescriptor mQueryDescriptor;

	protected AbstractStardogQueryProcessor(PropertyDescriptor queryNameDescriptor, PropertyDescriptor queryDescriptor) {
		mQueryNameDescriptor = queryNameDescriptor;
		mQueryDescriptor = queryDescriptor;
	}

	protected void validateQueryName(ValidationContext validationContext, Set<ValidationResult> results) {
		PropertyValue queryName = validationContext.getProperty(mQueryNameDescriptor);
		PropertyValue query = validationContext.getProperty(mQueryDescriptor);
		if (query.isSet() && queryName.isSet()) {
			results.add(new ValidationResult.Builder().valid(false)
			                                          .subject("Query")
			                                          .input(queryName.getValue())
			                                          .explanation("Cannot set both " +
			                                                       mQueryNameDescriptor.getDisplayName() +
			                                                       " and " +
			                                                       mQueryDescriptor.getDisplayName())
			                                          .build());
		}
		else if (!query.isSet() && !queryName.isSet()) {
			results.add(new ValidationResult.Builder().valid(false)
			                                          .subject("Query")
			                                          .explanation("Must set either " +
			                                                       mQueryNameDescriptor.getDisplayName() +
			                                                       " or " +
			                                                       mQueryDescriptor.getDisplayName())
			                                          .build());
		}
	}

	protected String getQueryString(ProcessContext context, FlowFile inputFile, Connection connection) {
		return context.getProperty(mQueryDescriptor).isSet()
		       ? context.getProperty(mQueryDescriptor).evaluateAttributeExpressions(inputFile).getValue()
		       : connection.admin()
		                   .getStoredQueries()
		                   .get(context.getProperty(mQueryNameDescriptor)
		                               .evaluateAttributeExpressions(inputFile)
		                               .getValue())
		                   .getQuery();
	}
}
