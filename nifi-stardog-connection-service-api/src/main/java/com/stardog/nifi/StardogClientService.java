package com.stardog.nifi;

import com.complexible.stardog.api.Connection;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public interface StardogClientService extends ControllerService {

	PropertyDescriptor.Builder SERVER_DESCRIPTOR_BUILDER = new PropertyDescriptor.Builder()
			.name("Stardog Connection String")
			.description("Fully qualified Stardog connection string containing the database name, e.g. http://localhost:5820/myDB.")
			.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
			.addValidator(StandardValidators.URL_VALIDATOR);

	PropertyDescriptor.Builder USERNAME_DESCRIPTOR_BUILDER = new PropertyDescriptor.Builder()
			.name("Username")
			.description("Username to connect to Stardog")
			.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR);

	PropertyDescriptor.Builder PASSWORD_DESCRIPTOR_BUILDER = new PropertyDescriptor.Builder()
			.name("Password")
			.description("Password to connect to Stardog")
			.sensitive(true)
			.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR);

	Connection connect();
}
