package com.stardog.nifi;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.metadata.MetaProperties;
import com.complexible.stardog.reasoning.ReasoningOptions;
import com.stardog.stark.IRI;
import com.stardog.stark.Values;

import com.google.api.client.util.Sets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.util.Strings;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import static com.stardog.nifi.StardogClientService.PASSWORD_DESCRIPTOR_BUILDER;
import static com.stardog.nifi.StardogClientService.SERVER_DESCRIPTOR_BUILDER;
import static com.stardog.nifi.StardogClientService.USERNAME_DESCRIPTOR_BUILDER;

/**
 * AbstractStardogProcessor is a base class that contains utility functions for Stardog processors like
 * connecting to a Stardog server.
 */
public abstract class AbstractStardogProcessor extends AbstractProcessor {

	static {
		// Needed to load reasoning.schemas property
		MetaProperties.register(ReasoningOptions.class);
	}

	static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
			.name("stardog-client-service")
			.displayName("Client Service")
			.description("If configured, this property will use the assigned client service for connection pooling.")
			.required(false)
			.identifiesControllerService(StardogClientService.class)
			.build();

	static final PropertyDescriptor SERVER = SERVER_DESCRIPTOR_BUILDER.required(false).build();

	static final PropertyDescriptor USERNAME = USERNAME_DESCRIPTOR_BUILDER.required(false).build();

	static final PropertyDescriptor PASSWORD = PASSWORD_DESCRIPTOR_BUILDER.required(false).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("A FlowFile is transferred to this relationship if the operation completed successfully.")
			.build();

	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("A FlowFile is transferred to this relationship if the operation failed.")
			.build();

	public static final List<PropertyDescriptor> DEFAULT_PROPERTIES = ImmutableList.of(CLIENT_SERVICE, SERVER, USERNAME, PASSWORD);

	public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
	                                                                       .description("A FlowFile is transferred to this relationship if the operation cannot be completed but attempting "
	                                                                                    + "it again may succeed.")
	                                                                       .build();

	public static final Set<Relationship> DEFAULT_RELATIONSHIPS = ImmutableSet.of(REL_SUCCESS, REL_FAILURE, REL_RETRY);

	protected ConnectionConfiguration getConnectionConfiguration(PropertyContext context) {
		ConnectionConfiguration configuration;
		StardogClientService stardogClientService = context.getProperty(CLIENT_SERVICE)
		                                                   .asControllerService(StardogClientService.class);

		if (stardogClientService == null) {
			String connectionURL = context.getProperty(SERVER).evaluateAttributeExpressions().getValue();
			String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
			String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
			configuration = ConnectionConfiguration.from(connectionURL)
			                                       .credentials(username, password);
		}
		else {
			configuration = stardogClientService.getConnectionConfiguration();
		}
		return configuration;
	}

	protected Connection connect(PropertyContext context) {
		return getConnectionConfiguration(context).connect();
	}

	@Override
	final protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
		Set<ValidationResult> results = Sets.newHashSet();

		if (!validationContext.getProperty(CLIENT_SERVICE).isSet()) {
			// Optional properties are required if service is not set
			validateProperty(validationContext, results, SERVER, StandardValidators.URI_VALIDATOR);
			validateProperty(validationContext, results, USERNAME, StandardValidators.NON_EMPTY_VALIDATOR);
			validateProperty(validationContext, results, PASSWORD, StandardValidators.NON_EMPTY_VALIDATOR);
		}

		customValidate(validationContext, results);

		return results;
	}

	private void validateProperty(ValidationContext validationContext, Set<ValidationResult> results, PropertyDescriptor descriptor, Validator validator) {
		String server = validationContext.getProperty(descriptor).getValue();
		ValidationResult result = validator.validate(descriptor.getDisplayName(), server, validationContext);
		if (!result.isValid()) {
			results.add(result);
		}
	}

	protected abstract void customValidate(ValidationContext validationContext, Set<ValidationResult> results);

	protected static final Validator IRI_VALIDATOR = (subject, input, context) -> {
		if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
			return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
		}
		else {
			try {
				Values.iri(input);
				return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Valid IRI").valid(true).build();
			}
			catch (Exception var5) {
				return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Not a valid IRI").valid(false).build();
			}
		}
	};

	public static IRI toIRI(String iri, Connection conn, IRI defaultIRI) {
		return Strings.isEmpty(iri)
		       ? defaultIRI
		       : Values.iri(conn.namespaces().map(iri).orElse(iri));
	}

	protected static FlowFile getOptionalFlowFile(ProcessContext context, ProcessSession session) {
		FlowFile inputFile = session.get();
		// If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
		// However, if we have no FlowFile and we have connections coming from other Processors, then
		// we know that we should run only if we have a FlowFile.
		if (inputFile == null) {
			if (context.hasNonLoopConnection()) {
				return null;
			}

			inputFile = session.create();
		}

		return inputFile;
	}
}
