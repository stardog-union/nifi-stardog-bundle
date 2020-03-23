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

import java.io.OutputStream;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.complexible.common.base.Objects2;
import com.complexible.common.rdf.query.SPARQLUtil;
import com.complexible.common.rdf.query.SPARQLUtil.QueryType;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.GraphQuery;
import com.complexible.stardog.api.Query;
import com.complexible.stardog.api.SelectQuery;
import com.stardog.stark.io.FileFormat;
import com.stardog.stark.io.RDFFormat;
import com.stardog.stark.io.RDFFormats;
import com.stardog.stark.io.RDFWriter;
import com.stardog.stark.io.RDFWriters;
import com.stardog.stark.query.GraphQueryResult;
import com.stardog.stark.query.SelectQueryResult;
import com.stardog.stark.query.io.QueryResultFormat;
import com.stardog.stark.query.io.QueryResultFormats;
import com.stardog.stark.query.io.QueryResultWriters;
import com.stardog.stark.query.io.SelectQueryResultWriter;

import com.google.api.client.util.Sets;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"stardog", "sparql", "update"})
@CapabilityDescription("Execute provided SPARQL update query over a Stardog database. This processor does not generate a result file. " +
                       "This processor can be scheduled to run on a timer, or cron expression, using the standard " +
                       "scheduling methods, or it can be triggered by an incoming FlowFile. If it is triggered by an incoming FlowFile, then " +
                       "attributes of that FlowFile will be available when evaluating the query but the contents of that file will not be used.")
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SeeAlso({})
public class StardogUpdate extends AbstractStardogProcessor {
    @Override
    protected void init(final ProcessorInitializationContext context) {

    }

    @Override
    public Set<Relationship> getRelationships() {
        return DEFAULT_RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DEFAULT_PROPERTIES;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inputFile = null;
        if (context.hasIncomingConnection()) {
            inputFile = session.get();
        }

        if (inputFile == null) {
            inputFile = session.create();
        }
    }

}
