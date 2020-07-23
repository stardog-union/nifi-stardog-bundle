package com.stardog.nifi;

import java.util.List;

import com.complexible.stardog.api.ConnectionConfiguration;

import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;

/**
 * A {@link ControllerService} that provides common options, such as credentials and connection string, for all Stardog {@link Processor processors}.
 */
@Tags({ "Credentials", "Authentication", "Security", "stardog" })
@CapabilityDescription("Provides a controller service that configures a connection to Stardog.")
public class StardogControllerService extends AbstractControllerService implements StardogClientService {

	static final PropertyDescriptor SERVER = SERVER_DESCRIPTOR_BUILDER.required(true).build();

	static final PropertyDescriptor USERNAME = USERNAME_DESCRIPTOR_BUILDER.required(true).build();

	static final PropertyDescriptor PASSWORD = PASSWORD_DESCRIPTOR_BUILDER.required(true).build();

	private static final List<PropertyDescriptor> SERVICE_PROPERTIES = ImmutableList.of(SERVER, USERNAME, PASSWORD);

	private String connectionURL;

	private String username;

	private String password;

	@Override
	protected void init(ControllerServiceInitializationContext config) throws InitializationException {
		super.init(config);
	}

	@OnEnabled
	public void onEnabled(ConfigurationContext context) {
		this.connectionURL = context.getProperty(SERVER).evaluateAttributeExpressions().getValue();
		this.username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
		this.password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return SERVICE_PROPERTIES;
	}

	@Override
	public ConnectionConfiguration getConnectionConfiguration() {
		return ConnectionConfiguration.from(connectionURL).credentials(username, password);
	}
}
