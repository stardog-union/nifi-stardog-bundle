package com.stardog.nifi;

import com.complexible.stardog.api.Connection;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public interface StardogClientService extends ControllerService {

	PropertyDescriptor SERVER = new PropertyDescriptor.Builder()
			.name("Stardog Connection String")
			.description("Fully qualified Stardog connection string containing the database name, e.g. http://localhost:5820/myDB.")
			.required(false)
			.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
			.addValidator(StandardValidators.URL_VALIDATOR)
			.build();

	PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
			.name("Username")
			.description("Username to connect to Stardog")
			.required(false)
			.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
			.name("Password")
			.description("Password to connect to Stardog")
			.required(false)
			.sensitive(true)
			.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	Connection connect();
}
