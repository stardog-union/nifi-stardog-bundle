// Copyright (c) 2010 - 2020, Stardog Union. <http://www.stardog.com>
// For more information about licensing and copyright of this software, please contact
// sales@stardog.com or visit http://stardog.com

package com.stardog.nifi;

import java.util.function.Supplier;

import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.ConnectionCredentials;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public interface StardogClientService extends ControllerService {

	PropertyDescriptor.Builder SERVER_DESCRIPTOR_BUILDER =
			new PropertyDescriptor.Builder().name("Stardog Connection String")
			                                .description("Fully qualified Stardog connection string containing " +
			                                             "the database name, e.g. http://localhost:5820/myDB.")
			                                .addValidator(StandardValidators.URL_VALIDATOR);

	PropertyDescriptor USERNAME =
			new PropertyDescriptor.Builder().name("Username")
			                                .description("Username to connect to Stardog")
			                                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
			                                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			                                .required(false)
			                                .build();

	PropertyDescriptor PASSWORD =
			new PropertyDescriptor.Builder().name("Password")
			                                .description("Password to connect to Stardog")
			                                .sensitive(true)
			                                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
			                                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			                                .required(false)
			                                .build();

	ConnectionConfiguration getConnectionConfiguration();

	boolean isCredentialsSet();

	/**
	 * A concrete class for implementing {@link Supplier<ConnectionCredentials>} for testing purposes
	 */
	class UsernamePasswordSupplier implements Supplier<ConnectionCredentials> {

		private final String mUsername;

		private final String mPassword;

		public UsernamePasswordSupplier(String username, String password) {
			mUsername = username;
			mPassword = password;
		}

		@Override
		public ConnectionCredentials get() {
			return ConnectionCredentials.getUsernamePasswordCredential(mUsername, mPassword);
		}

		public String getUsername() {
			return mUsername;
		}

		public String getPassword() {
			return String.valueOf(mPassword);
		}
	}
}
