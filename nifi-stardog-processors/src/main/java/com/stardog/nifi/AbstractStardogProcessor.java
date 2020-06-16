package com.stardog.nifi;

import java.util.List;
import java.util.Set;

import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.stardog.stark.IRI;
import com.stardog.stark.Values;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.util.Strings;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import static com.stardog.nifi.StardogClientService.PASSWORD_DESCRIPTOR_BUILDER;
import static com.stardog.nifi.StardogClientService.SERVER_DESCRIPTOR_BUILDER;
import static com.stardog.nifi.StardogClientService.USERNAME_DESCRIPTOR_BUILDER;

/**
 * AbstractStardogProcessor is a base class that contains utility functions for Stardog processors like
 * connecting to a Stardog server.
 */
public abstract class AbstractStardogProcessor extends AbstractProcessor {

	private static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
			.name("stardog-client-service")
			.displayName("Client Service")
			.description("If configured, this property will use the assigned client service for connection pooling.")
			.required(false)
			.identifiesControllerService(StardogClientService.class)
			.build();

	private static final PropertyDescriptor SERVER = SERVER_DESCRIPTOR_BUILDER.required(false).build();

	private static final PropertyDescriptor USERNAME = USERNAME_DESCRIPTOR_BUILDER.required(false).build();

	private static final PropertyDescriptor PASSWORD = PASSWORD_DESCRIPTOR_BUILDER.required(false).build();

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

	protected Connection connect(ProcessContext context) {
		StardogClientService stardogClientService = context.getProperty(CLIENT_SERVICE)
		                                                   .asControllerService(StardogClientService.class);

		if (stardogClientService == null) {
			String connectionURL = getNonNullProperty(context, SERVER);
			String username = getNonNullProperty(context, USERNAME);
			String password = getNonNullProperty(context, PASSWORD);
			return ConnectionConfiguration.from(connectionURL)
			                              .credentials(username, password)
			                              .connect();
		}
		else {
			return stardogClientService.connect();
		}
	}

	private String getNonNullProperty(ProcessContext context, PropertyDescriptor descriptor) {
		String value = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
		if (value == null || value.isEmpty()) {
			throw new IllegalArgumentException(descriptor.getDisplayName() + " property must be set.");
		}
		return value;
	}

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

	public static FlowFile getOptionalFlowFile(ProcessContext context, ProcessSession session) {
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
