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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * AbstractStardogProcessor is a base class that contains utility functions for Stardog processors like
 * connecting to a Stardog server.
 */
public abstract class AbstractStardogProcessor extends AbstractProcessor {
    public static final PropertyDescriptor SERVER = new PropertyDescriptor.Builder()
            .name("Stardog Connection String")
            .description("Fully qualified Stardog connection string containing the database name, e.g. http://localhost:5820/myDB.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to connect to Stardog")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to connect to Stardog")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final  List<PropertyDescriptor> DEFAULT_PROPERTIES = ImmutableList.of(SERVER, USERNAME,PASSWORD);

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred to this relationship if the operation completed successfully.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is transferred to this relationship if the operation failed.")
            .build();
    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is transferred to this relationship if the operation cannot be completed but attempting "
                    + "it again may succeed.")
            .build();

    public static final  Set<Relationship> DEFAULT_RELATIONSHIPS = ImmutableSet.of(REL_SUCCESS, REL_FAILURE, REL_RETRY);

    protected Connection connect(ProcessContext context) {
        // TODO Use connection pooling

        ComponentLog log = getLogger();
        String connectionURL = context.getProperty(SERVER).evaluateAttributeExpressions().getValue();

        String username= context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();


        Connection connection = ConnectionConfiguration.from(connectionURL)
                                            .credentials(username, password)
                                            .connect();

        log.info("Connected to Stardog: {}", new Object[] { connectionURL });

        return connection;
    }

    protected static final Validator IRI_VALIDATOR = (subject, input, context) -> {
        if(context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
        } else {
            try {
                Values.iri(input);
                return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Valid IRI").valid(true).build();
            } catch (Exception var5) {
                return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Not a valid IRI").valid(false).build();
            }
        }
    };

    public static IRI toIRI(String iri, Connection conn, IRI defaultIRI) {
        return Strings.isEmpty(iri)
               ? defaultIRI
               : Values.iri(conn.namespaces().map(iri).orElse(iri));
    }

    public static FlowFile getOptionalFlowFile(final ProcessContext context, final ProcessSession session) {
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
