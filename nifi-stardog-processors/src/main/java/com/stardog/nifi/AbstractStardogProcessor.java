// Copyright (c) 2010 - 2020, Stardog Union. <http://www.stardog.com>
// For more information about licensing and copyright of this software, please contact
// sales@stardog.com or visit http://stardog.com

package com.stardog.nifi;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.ConnectionCredentials;
import com.complexible.stardog.api.LoginConnectionConfiguration;
import com.complexible.stardog.metadata.MetaProperties;
import com.complexible.stardog.reasoning.ReasoningOptions;
import com.stardog.stark.IRI;
import com.stardog.stark.Values;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import static com.stardog.nifi.StardogClientService.PASSWORD;
import static com.stardog.nifi.StardogClientService.SERVER_DESCRIPTOR_BUILDER;
import static com.stardog.nifi.StardogClientService.USERNAME;
import static com.stardog.nifi.StardogClientService.UsernamePasswordSupplier;

/**
 * AbstractStardogProcessor is a base class that contains utility functions for Stardog processors like
 * connecting to a Stardog server.
 */
public abstract class AbstractStardogProcessor extends AbstractProcessor {

	static {
		// Needed to load reasoning.schemas property
		MetaProperties.register(ReasoningOptions.class);
	}

	static final PropertyDescriptor CLIENT_SERVICE =
			new PropertyDescriptor.Builder()
					.name("stardog-client-service")
					.displayName("Client Service")
					.description("If configured, this processor will use the assigned client service for connection pooling.")
					.required(false)
					.identifiesControllerService(StardogClientService.class)
					.build();

	static final PropertyDescriptor SERVER = SERVER_DESCRIPTOR_BUILDER.required(false).build();

	/**
	 * The use of {@link KerberosCredentialsService} allows the separation of the specification of the keytab file
	 * from the other processor properties. If we used processor properties then the user would have to specify the
	 * path to the keytab file, which would give them the power to select any of the keytab files that are accessible
	 * to the nifi server, defeating the secrecy a keytab requires. By using the service, an administrator can create
	 * a processor group (PG) for each principal. By controlling who has access to each PG, the admin can control
	 * which users use each keytab.
	 * See <a href="https://bryanbende.com/development/2018/04/09/apache-nifi-secure-keytab-access"> Apache NiFi -
	 * Secure Keytab Access</a> by Bryan Bende.
	 * <p/>
	 * To create the keytab file on Windows:<br>
	 * {@code ktpass /princ NIFI/computername.home@SERVER.DOMAIN /mapuser username@server.domain /pass
	 * yourpassword /crypto RC4-HMAC-NT /ptype KRB5_NT_PRINCIPAL /out filename.keytab}
	 * <p/>
	 * Principal will be {@code NIFI/computername.home@SERVER.DOMAIN}, which must added as a Stardog user.
	 */
	static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE =
			new PropertyDescriptor.Builder()
					.name("kerberos-credentials-service")
					.displayName("Kerberos Credentials Service")
					.description("Specifies the Kerberos Credentials Controller Service that should be used for " +
					             "authenticating with Kerberos. If set, " + USERNAME.getDisplayName() + " and " +
					             PASSWORD.getDisplayName() + " can be empty.")
					.identifiesControllerService(KerberosCredentialsService.class)
					.required(false)
					.build();

	public static final Relationship REL_SUCCESS =
			new Relationship.Builder().name("success")
			                          .description("A FlowFile is transferred to this relationship if the operation completed successfully.")
			                          .build();

	public static final Relationship REL_FAILURE =
			new Relationship.Builder().name("failure")
			                          .description("A FlowFile is transferred to this relationship if the operation failed.")
			                          .build();

	public static final List<PropertyDescriptor> DEFAULT_PROPERTIES =
			ImmutableList.of(CLIENT_SERVICE, KERBEROS_CREDENTIALS_SERVICE, SERVER, USERNAME, PASSWORD);

	public static final Relationship REL_RETRY =
			new Relationship.Builder().name("retry")
			                          .description("A FlowFile is transferred to this relationship if the operation " +
			                                       "cannot be completed but attempting it again may succeed.")
			                          .build();

	public static final Set<Relationship> DEFAULT_RELATIONSHIPS = ImmutableSet.of(REL_SUCCESS, REL_FAILURE, REL_RETRY);

	protected ConnectionConfiguration getConnectionConfiguration(PropertyContext context, FlowFile inputFile) {
		ConnectionConfiguration configuration;
		StardogClientService stardogClientService = context.getProperty(CLIENT_SERVICE)
		                                                   .asControllerService(StardogClientService.class);

		String connectionURL = context.getProperty(SERVER).evaluateAttributeExpressions(inputFile).getValue();
		if (stardogClientService == null) {
			String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
			String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
			configuration = ConnectionConfiguration
					.from(connectionURL)
					.credentialSupplier(new UsernamePasswordSupplier(username, password));
		}
		else {
			configuration = stardogClientService.getConnectionConfiguration();

			// We let the user override the url in the processor so that expressions can be applied
			if (connectionURL != null) {
				ConnectionConfiguration override = ConnectionConfiguration.from(connectionURL);
				configuration.server(override.get(LoginConnectionConfiguration.SERVER))
				             .database(override.get(ConnectionConfiguration.DATABASE));
			}
		}

		KerberosCredentialsService krb5CredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE)
		                                                           .asControllerService(KerberosCredentialsService.class);
		if (krb5CredentialsService != null) {
			String keytab = krb5CredentialsService.getKeytab();
			String principal = krb5CredentialsService.getPrincipal();
			configuration = configuration.credentialSupplier(new Krb5CredentialsSupplier(keytab, principal));
		}
		return configuration;
	}

	protected Connection connect(PropertyContext context, FlowFile inputFile) {
		return getConnectionConfiguration(context, inputFile).connect();
	}

	/**
	 * Perform validation on the properties that are common to all Stardog processors. Common rules are:<br>
	 * <p><ul>
	 * <li>{@link #SERVER Connection URL} is required unless {@link #CLIENT_SERVICE} is set
	 * <li>{@link #CLIENT_SERVICE} must include credentials unless {@link #KERBEROS_CREDENTIALS_SERVICE} is set
	 * <li>{@link StardogClientService#USERNAME} and {@link StardogClientService#PASSWORD} are required unless
	 * {@link #CLIENT_SERVICE} or {@link #KERBEROS_CREDENTIALS_SERVICE} is set
	 * </ul><p>
	 * This validation would be simpler if we set krb5 in the client service, but that option was rejected because 1)
	 * it's good to conform with NiFi's existing mechanism for setting krb5 and 2) the pattern for the client service
	 * properties is to also have those properties in each processor and we want to avoid giving the user the ability
	 * to select the keytab location at the processor config level. See: {@link #KERBEROS_CREDENTIALS_SERVICE}
	 */
	@Override
	final protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
		Set<ValidationResult> results = Sets.newHashSet();

		// Optional properties are required if service is not set
		PropertyValue clientService = validationContext.getProperty(CLIENT_SERVICE);
		if (!clientService.isSet()) {
			validateProperty(validationContext, results, SERVER, StandardValidators.URI_VALIDATOR);
		}

		if (!validationContext.getProperty(KERBEROS_CREDENTIALS_SERVICE).isSet()) {
			if (clientService.isSet()) {
				if (!clientService.asControllerService(StardogClientService.class).isCredentialsSet()) {
					results.add(new ValidationResult.Builder()
							.subject(CLIENT_SERVICE.getDisplayName())
							.valid(false)
							.explanation(CLIENT_SERVICE.getDisplayName() +
							             " must have credentials set when " +
							             KERBEROS_CREDENTIALS_SERVICE.getDisplayName() +
							             " is not set")
							.build());
				}
			}
			else {
				validateProperty(validationContext, results, USERNAME, StandardValidators.NON_EMPTY_VALIDATOR);
				validateProperty(validationContext, results, PASSWORD, StandardValidators.NON_EMPTY_VALIDATOR);
			}
		}

		customValidate(validationContext, results);

		return results;
	}

	private void validateProperty(ValidationContext validationContext, Set<ValidationResult> results, PropertyDescriptor descriptor, Validator validator) {
		String value = validationContext.getProperty(descriptor).getValue();
		ValidationResult result = validator.validate(descriptor.getDisplayName(), value, validationContext);
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
		return Strings.isNullOrEmpty(iri)
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

	/**
	 * A concrete class for implementing {@link Supplier<ConnectionCredentials>} for testing purposes
	 */
	public static class Krb5CredentialsSupplier implements Supplier<ConnectionCredentials> {

		private final String mKeytab;

		private final String mPrincipal;

		public Krb5CredentialsSupplier(String keytab, String principal) {
			mKeytab = keytab;
			mPrincipal = principal;
		}

		@Override
		public ConnectionCredentials get() {
			return ConnectionCredentials.createKrb5Credential(new File(mKeytab), mPrincipal, true);
		}

		public String getPrincipal() {
			return mPrincipal;
		}
	}
}
