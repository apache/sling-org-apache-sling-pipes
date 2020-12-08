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

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.pipes.SuperPipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * executes a pipe referred in the configuration
 */
public class ReferencePipe extends SuperPipe {
    private static final Logger log = LoggerFactory.getLogger(ReferencePipe.class);

    public static final String RESOURCE_TYPE = "slingPipes/reference";
    static final String PN_SKIPEXECUTION = "skipExecution";

    protected Pipe reference;

    /**
     * path of the reference pipe
     */
    String referencePath;

    public ReferencePipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
    }

    @Override
    public void buildChildren() {
        String expression = getExpr();
        if (StringUtils.isNotBlank(expression) && !expression.equals(referencePath)) {
            referencePath = expression;
            Resource pipeResource = resolver.getResource(referencePath);
            if (pipeResource == null) {
                pipeResource = plumber.getReferencedResource(resource, referencePath);
            }
            if (pipeResource == null) {
                throw new IllegalArgumentException("Reference configuration error: There is no resource at " + referencePath);
            }
            reference = plumber.getPipe(pipeResource, bindings);
            if (reference == null) {
                throw new IllegalArgumentException("Unable to build out pipe out of " + referencePath);
            }
            reference.setParent(this);
            log.info("sets reference to {}", reference);

            subpipes.clear();
            subpipes.add(reference);
        }
    }

    /**
     * @return boolean flag mentionning wether we could skip the execution of that pipe
     */
    boolean shouldBeSkipped() {
        String skip = properties.get(PN_SKIPEXECUTION, String.class);
        if (StringUtils.isNotBlank(skip)) {
            Object object = bindings.instantiateObject(skip);
            if (object instanceof Boolean) {
                return (Boolean)object;
            }
        }
        return false;
    }

    protected Iterator<Resource> computeReferredOutput() {
        buildChildren();
        log.debug("getting {} output", reference);
        return reference.getOutput();
    }

    @Override
    protected Iterator<Resource> computeSubpipesOutput() {
        if (! shouldBeSkipped()) {
            return computeReferredOutput();
        }
        return EMPTY_ITERATOR;
    }
}