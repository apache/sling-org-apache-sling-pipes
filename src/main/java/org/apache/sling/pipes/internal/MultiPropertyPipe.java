/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sling.pipes.internal;

import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.Value;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

/**
 * reads input MV property, outputs N times the input parent node resource, where N is the number of
 * values in the property, outputs each value in the bindings
 */
public class MultiPropertyPipe extends BasePipe {
    private static Logger logger = LoggerFactory.getLogger(MultiPropertyPipe.class);
    public static final String RESOURCE_TYPE = RT_PREFIX + "multiProperty";

    public MultiPropertyPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
    }

    MVResourceIterator iterator;

    @Override
    protected Iterator<Resource> computeOutput() {
        iterator = new MVResourceIterator(getInput());
        return iterator;
    }

    @Override
    public Object getOutputBinding() {
        if (iterator != null) {
            Value value = iterator.getCurrentValue();
            String output = value.toString();
            try {
                if (PropertyType.STRING == value.getType()) {
                    output = value.getString();
                }
            } catch (Exception e) {
                logger.error("current value format is not supported", e);
            }
            return output;
        }
        return null;
    }

    static class MVResourceIterator implements Iterator<Resource> {
        Resource resource;
        Value currentValue;
        Iterator<Value> itValue = Collections.emptyIterator();

        public MVResourceIterator(Resource resource){
            this.resource = resource;
            if (this.resource != null) {
                try {
                    Property mvProperty = resource.adaptTo(Property.class);
                    if (mvProperty == null) {
                        throw new IllegalArgumentException("input resource " + resource.getPath() + " is supposed to be a property");
                    }
                    if (!mvProperty.isMultiple()) {
                        throw new IllegalArgumentException("given property " + resource.getPath() + " is supposed to be multiple");
                    }
                    itValue = Arrays.asList(mvProperty.getValues()).iterator();
                } catch (Exception e) {
                    logger.warn("unable to setup mv iterator for resource, will return nothing", e);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return itValue.hasNext();
        }

        public Value getCurrentValue() {
            return currentValue;
        }

        @Override
        public Resource next() {
            if (itValue.hasNext()) {
                currentValue = itValue.next();
            }
            return resource;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
