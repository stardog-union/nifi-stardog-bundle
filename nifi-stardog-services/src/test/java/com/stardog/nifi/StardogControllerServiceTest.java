// Copyright (c) 2010 - 2020, Stardog Union. <http://www.stardog.com>
// For more information about licensing and copyright of this software, please contact
// sales@stardog.com or visit http://stardog.com

package com.stardog.nifi;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class StardogControllerServiceTest {

    public static final String CONTROLLER_SERVICE_NAME = "Client Service";

    private TestRunner runner;
    private StardogControllerService service;

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new StardogControllerService();
        runner.addControllerService(CONTROLLER_SERVICE_NAME, service);
    }

    @Test
    public void testServiceValidation() throws Exception {
        runner.assertNotValid(service);

        unsetServiceProperties();
        runner.setProperty(service, StardogControllerService.SERVER, "http://localhost:1234/foo");
        // We won't know until the processor is configured whether krb5 is configured
        runner.assertValid(service);

        unsetServiceProperties();
        runner.setProperty(service, StardogControllerService.USERNAME, "username");
        runner.assertNotValid(service);

        unsetServiceProperties();
        runner.setProperty(service, StardogControllerService.PASSWORD, "password");
        runner.assertNotValid(service);

        setServiceProperties();
        runner.assertValid(service);
    }

    @Test
    public void testProcessorValidation() {
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, CONTROLLER_SERVICE_NAME);
        runner.assertNotValid();

        setServiceProperties();
        runner.assertNotValid();

        runner.enableControllerService(service);
        runner.assertValid();
    }

    private void unsetServiceProperties() throws Exception {
        service = new StardogControllerService();
        runner.addControllerService(CONTROLLER_SERVICE_NAME, service);
    }

    private void setServiceProperties() {
        runner.setProperty(service, StardogControllerService.SERVER, "http://localhost:1234/foo");
        runner.setProperty(service, StardogControllerService.USERNAME, "username");
        runner.setProperty(service, StardogControllerService.PASSWORD, "password");
    }
}
