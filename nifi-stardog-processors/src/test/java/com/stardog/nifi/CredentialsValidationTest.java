// Copyright (c) 2010 - 2020, Stardog Union. <http://www.stardog.com>
// For more information about licensing and copyright of this software, please contact
// sales@stardog.com or visit http://stardog.com

package com.stardog.nifi;

import java.util.function.Supplier;

import com.complexible.common.base.Options;
import com.complexible.stardog.api.AbstractConnectionConfiguration;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.ConnectionCredentials;
import com.stardog.nifi.AbstractStardogProcessor.Krb5CredentialsSupplier;
import com.stardog.nifi.StardogClientService.UsernamePasswordSupplier;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static com.stardog.nifi.AbstractStardogProcessor.CLIENT_SERVICE;
import static com.stardog.nifi.AbstractStardogProcessor.KERBEROS_CREDENTIALS_SERVICE;
import static com.stardog.nifi.AbstractStardogProcessor.SERVER;
import static com.stardog.nifi.StardogClientService.PASSWORD;
import static com.stardog.nifi.StardogClientService.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class CredentialsValidationTest extends AbstractStardogProcessorTest {

	public static final String KERBEROS_PRINCIPAL = "NIFI/computername.home@SERVER.DOMAIN";

	@Override
	protected Class<? extends AbstractStardogProcessor> getProcessorClass() {
		return StardogPut.class;
	}

	@Override
	protected TestRunner newTestRunner() {
		TestRunner runner = TestRunners.newTestRunner(getProcessorClass());
		runner.setProperty(SERVER, getStardogEndpoint());
		return runner;
	}

	@Test
	public void noCredentials() {
		TestRunner runner = newTestRunner();

		assertValidationResults(runner,
				"'Username' is invalid because Username cannot be empty",
				"'Password' is invalid because Password cannot be empty");
	}

	@Test
	public void credsInProcessor() {
		TestRunner runner = newTestRunner();

		runner.setProperty(USERNAME, "testuser");
		runner.setProperty(PASSWORD, "testpass");

		runner.assertValid();

		Supplier<ConnectionCredentials> credentialsSupplier = getConnectionCredentials(runner);
		assertThat(credentialsSupplier, CoreMatchers.instanceOf(UsernamePasswordSupplier.class));
		UsernamePasswordSupplier usernamePasswordSupplier = (UsernamePasswordSupplier) credentialsSupplier;
		assertEquals("testuser", usernamePasswordSupplier.getUsername());
		assertEquals("testpass", usernamePasswordSupplier.getPassword());
	}

	@Test
	public void krb5Only() throws InitializationException {
		TestRunner runner = newTestRunner();

		runner.setProperty(KERBEROS_CREDENTIALS_SERVICE, "Krb5Service");
		MockKerberosCredentialsService kerberosCredentialsService = new MockKerberosCredentialsService();
		runner.addControllerService("Krb5Service", kerberosCredentialsService);
		runner.enableControllerService(kerberosCredentialsService);

		runner.assertValid();

		Supplier<ConnectionCredentials> credentialsSupplier = getConnectionCredentials(runner);
		assertThat(credentialsSupplier, CoreMatchers.instanceOf(Krb5CredentialsSupplier.class));
		Krb5CredentialsSupplier krb5CredentialsSupplier = (Krb5CredentialsSupplier) credentialsSupplier;
		assertEquals(KERBEROS_PRINCIPAL, krb5CredentialsSupplier.getPrincipal());
	}

	@Test
	public void clientServiceWithoutCreds() throws InitializationException {
		TestRunner runner = newTestRunner();

		runner.setProperty(CLIENT_SERVICE, "StardogService");
		StardogClientService clientService = new StardogControllerService();
		runner.addControllerService("StardogService", clientService);
		runner.setProperty(clientService, StardogControllerService.SERVER, "http://localhost:1234/foo");
		runner.enableControllerService(clientService);

		assertSingleValidationResult(runner, "'Client Service' is invalid because Client Service must have " +
		                                     "credentials set when Kerberos Credentials Service is not set");
	}

	@Test
	public void clientServiceCredsInService() throws InitializationException {
		TestRunner runner = newTestRunner();

		runner.setProperty(CLIENT_SERVICE, "StardogService");
		StardogClientService clientService = new StardogControllerService();
		runner.addControllerService("StardogService", clientService);
		runner.setProperty(clientService, StardogControllerService.SERVER, "http://localhost:1234/foo");
		runner.setProperty(clientService, USERNAME, "testuser");
		runner.setProperty(clientService, PASSWORD, "testpass");
		runner.enableControllerService(clientService);

		runner.setProperty(USERNAME, "should_ignore");
		runner.setProperty(PASSWORD, "should_ignore");

		runner.assertValid();

		Supplier<ConnectionCredentials> credentialsSupplier = getConnectionCredentials(runner);
		assertThat(credentialsSupplier, CoreMatchers.instanceOf(UsernamePasswordSupplier.class));
		UsernamePasswordSupplier usernamePasswordSupplier = (UsernamePasswordSupplier) credentialsSupplier;
		assertEquals("testuser", usernamePasswordSupplier.getUsername());
		assertEquals("testpass", usernamePasswordSupplier.getPassword());
	}

	@Test
	public void clientServiceCredsInProcessor() throws InitializationException {
		TestRunner runner = newTestRunner();

		runner.setProperty(CLIENT_SERVICE, "StardogService");
		StardogClientService clientService = new StardogControllerService();
		runner.addControllerService("StardogService", clientService);
		runner.setProperty(clientService, StardogControllerService.SERVER, "http://localhost:1234/foo");
		runner.enableControllerService(clientService);

		runner.setProperty(USERNAME, "testuser");
		runner.setProperty(PASSWORD, "testpass");

		assertSingleValidationResult(runner, "'Client Service' is invalid because Client Service must have " +
		                                     "credentials set when Kerberos Credentials Service is not set");
	}

	@Test
	public void clientServiceAndKrb5() throws InitializationException {
		TestRunner runner = newTestRunner();

		runner.setProperty(CLIENT_SERVICE, "StardogService");
		StardogClientService clientService = new StardogControllerService();
		runner.addControllerService("StardogService", clientService);
		runner.setProperty(clientService, StardogControllerService.SERVER, "http://localhost:1234/foo");
		runner.enableControllerService(clientService);

		runner.setProperty(KERBEROS_CREDENTIALS_SERVICE, "Krb5Service");
		MockKerberosCredentialsService krb5CredentialsService = new MockKerberosCredentialsService();
		runner.addControllerService("Krb5Service", krb5CredentialsService);
		runner.enableControllerService(krb5CredentialsService);

		runner.assertValid();

		Supplier<ConnectionCredentials> credentialsSupplier = getConnectionCredentials(runner);
		assertThat(credentialsSupplier, CoreMatchers.instanceOf(Krb5CredentialsSupplier.class));
		Krb5CredentialsSupplier krb5CredentialsSupplier = (Krb5CredentialsSupplier) credentialsSupplier;
		assertEquals(KERBEROS_PRINCIPAL, krb5CredentialsSupplier.getPrincipal());
	}

	@Test
	public void clientServiceAllCreds() throws InitializationException {
		TestRunner runner = newTestRunner();

		runner.setProperty(CLIENT_SERVICE, "StardogService");
		StardogClientService clientService = new StardogControllerService();
		runner.addControllerService("StardogService", clientService);
		runner.setProperty(clientService, StardogControllerService.SERVER, "http://localhost:1234/foo");
		runner.setProperty(clientService, USERNAME, "testuser");
		runner.setProperty(clientService, PASSWORD, "testpass");
		runner.enableControllerService(clientService);

		runner.setProperty(KERBEROS_CREDENTIALS_SERVICE, "Krb5Service");
		MockKerberosCredentialsService krb5CredentialsService = new MockKerberosCredentialsService();
		runner.addControllerService("Krb5Service", krb5CredentialsService);
		runner.enableControllerService(krb5CredentialsService);

		runner.setProperty(USERNAME, "testuser");
		runner.setProperty(PASSWORD, "testpass");

		runner.assertValid();

		Supplier<ConnectionCredentials> credentialsSupplier = getConnectionCredentials(runner);
		assertThat(credentialsSupplier, CoreMatchers.instanceOf(Krb5CredentialsSupplier.class));
		Krb5CredentialsSupplier krb5CredentialsSupplier = (Krb5CredentialsSupplier) credentialsSupplier;
		assertEquals(KERBEROS_PRINCIPAL, krb5CredentialsSupplier.getPrincipal());
	}

	private Supplier<ConnectionCredentials> getConnectionCredentials(TestRunner runner) {
		ProcessContext context = runner.getProcessContext();
		StardogPut stardogPut = (StardogPut) runner.getProcessor();
		ConnectionConfiguration connectionConfiguration = stardogPut.getConnectionConfiguration(context);
		Options options = connectionConfiguration.getOptions();
		return options.get(AbstractConnectionConfiguration.CREDENTIALS_SUPPLIER);
	}

	static class MockKerberosCredentialsService extends AbstractControllerService implements KerberosCredentialsService {
		public MockKerberosCredentialsService() {
		}

		@Override
		public String getKeytab() {
			return "src/test/resources/filename.keytab";
		}

		@Override
		public String getPrincipal() {
			return KERBEROS_PRINCIPAL;
		}
	}
}