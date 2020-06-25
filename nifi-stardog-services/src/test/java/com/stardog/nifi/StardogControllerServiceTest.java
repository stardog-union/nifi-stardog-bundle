/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        runner.assertNotValid(service);

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
