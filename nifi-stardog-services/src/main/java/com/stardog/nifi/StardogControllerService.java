package com.stardog.nifi;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.complexible.stardog.api.ConnectionConfiguration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;

import static org.apache.nifi.expression.ExpressionLanguageScope.VARIABLE_REGISTRY;

/**
 * A {@link ControllerService} that provides common options, such as credentials and connection string, for all
 * Stardog {@link Processor processors}.
 */
@Tags({ "Credentials", "Authentication", "Security", "stardog" })
@CapabilityDescription("Provides a controller service that configures a connection to Stardog.")
public class StardogControllerService extends AbstractControllerService implements StardogClientService {

	static final PropertyDescriptor SERVER = SERVER_DESCRIPTOR_BUILDER
			.required(true)
			.expressionLanguageSupported(VARIABLE_REGISTRY)
			.build();

	private static final List<PropertyDescriptor> SERVICE_PROPERTIES = ImmutableList.of(SERVER, USERNAME, PASSWORD);

	private String connectionURL;

	private String username;

	private String password;

	@Override
	protected void init(ControllerServiceInitializationContext context) throws InitializationException {
		super.init(context);
	}

	@OnEnabled
	public void onEnabled(ConfigurationContext context) {
		this.connectionURL = context.getProperty(SERVER).evaluateAttributeExpressions().getValue();
		this.username = evaluatePropertyValueOrNull(context, USERNAME);
		this.password = evaluatePropertyValueOrNull(context, PASSWORD);
	}

	private String evaluatePropertyValueOrNull(ConfigurationContext context, PropertyDescriptor descriptor) {
		PropertyValue property = context.getProperty(descriptor);
		return property.isSet() ? property.evaluateAttributeExpressions().getValue() : null;
	}

	@Override
	protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
		Set<ValidationResult> results = Sets.newHashSet();

		PropertyValue username = validationContext.getProperty(USERNAME);
		PropertyValue password = validationContext.getProperty(PASSWORD);

		if (username.isSet() && !password.isSet()) {
			results.add(new ValidationResult.Builder().subject(PASSWORD.getDisplayName())
			                                          .valid(false)
			                                          .explanation("Must set " + PASSWORD.getDisplayName() +
			                                                       " when " + USERNAME + " is set.")
			                                          .build());
		}
		else if (!username.isSet() && password.isSet()) {
			results.add(new ValidationResult.Builder().subject(PASSWORD.getDisplayName())
			                                          .valid(false)
			                                          .explanation("Cannot set " + PASSWORD.getDisplayName() +
			                                                       " unless " + USERNAME + " is also set.")
			                                          .build());
		}
		return results;
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return SERVICE_PROPERTIES;
	}

	@Override
	public ConnectionConfiguration getConnectionConfiguration() {
		return ConnectionConfiguration.from(connectionURL)
		                              .credentialSupplier(new UsernamePasswordSupplier(username, password));
	}

	@Override
	public boolean isCredentialsSet() {
		return username != null && password != null;
	}
}
