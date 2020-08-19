// Copyright (c) 2010 - 2020, Stardog Union. <http://www.stardog.com>
// For more information about licensing and copyright of this software, please contact
// sales@stardog.com or visit http://stardog.com

package com.stardog.nifi;

import java.util.Collections;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

public class TestControllerServiceProcessor extends AbstractProcessor {

    static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Client Service")
            .description("StardogClientService")
            .identifiesControllerService(StardogControllerService.class)
            .required(true)
            .build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(CLIENT_SERVICE);
    }
}
