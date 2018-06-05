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
package org.apache.sling.pipes;

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.Iterator;

/**
 * executes a pipe referred in the configuration
 */
public class ReferencePipe extends BasePipe {
    private static final Logger log = LoggerFactory.getLogger(ReferencePipe.class);

    public static final String RESOURCE_TYPE = "slingPipes/reference";

    protected Pipe reference;

    /**
     * path of the reference pipe
     */
    String referencePath;

    public ReferencePipe(Plumber plumber, Resource resource) throws Exception {
        super(plumber, resource);
    }

    @Override
    public PipeBindings getBindings() {
        return reference.getBindings();
    }

    /**
     * Computing the pipe this pipe refers to, and make necessary bindings
     * @throws Exception
     */
    protected void computeReference() throws Exception {
        Resource pipeResource = resolver.getResource(referencePath);
        if (pipeResource == null) {
            throw new Exception("Reference configuration error: There is no resource at " + getExpr());
        }
        reference = plumber.getPipe(pipeResource);
        if (reference == null) {
            throw new Exception("Unable to build out pipe out of " + getPath());
        }
        reference.setReferrer(this);
        log.info("set reference to {}", reference);

        //bind parent to the reference
        if (parent != null) {
            reference.setParent(parent);
        }
        //set reference's bindings
        if (bindings != null) {
            reference.setBindings(bindings);
        }
    }

    @Override
    protected Iterator<Resource> computeOutput() throws Exception {
        String expression = getExpr();
        if (StringUtils.isNotBlank(expression) && !expression.equals(referencePath)){
            referencePath = expression;
            computeReference();
        }
        return computeReferenceOutput();
    }

    /**
     * @return referenced pipe output
     * @throws Exception sent by reference piped execution
     */
    protected Iterator<Resource> computeReferenceOutput() throws Exception {
        log.debug("getting {} output", reference);
        return reference.getOutput();
    }

    @Override
    public Object getOutputBinding() {
        return reference.getOutputBinding();
    }

    @Override
    public boolean modifiesContent() {
        //assuming true in case we don't know yet to be on the safe side
        return reference != null ? reference.modifiesContent() : true;
    }
}