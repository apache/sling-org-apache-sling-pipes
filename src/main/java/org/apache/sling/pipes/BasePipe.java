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
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * provides generic utilities for a pipe, is also a dummy pipe (outputs its input, without changing anything)
 */
public class BasePipe implements Pipe {

    private final Logger logger = LoggerFactory.getLogger(BasePipe.class);
    public static final String SLASH = "/";
    public static final String RT_PREFIX = "slingPipes/";
    public static final String RESOURCE_TYPE = RT_PREFIX + "base";
    public static final String DRYRUN_KEY = "dryRun";
    public static final String READ_ONLY = "readOnly";
    public static final String PN_STATUS = "status";
    public static final String PN_STATUS_MODIFIED = "statusModified";
    public static final String STATUS_STARTED = "started";
    public static final String STATUS_FINISHED = "finished";
    protected static final String DRYRUN_EXPR = String.format("${%s}", DRYRUN_KEY);

    protected ResourceResolver resolver;
    protected ValueMap properties;
    protected Resource resource;
    protected ContainerPipe parent;
    protected String distributionAgent;
    protected PipeBindings bindings;
    protected ReferencePipe referrer;

    // used by pipes using complex JCR configurations
    public static final List<String> IGNORED_PROPERTIES = Arrays.asList(new String[]{"jcr:lastModified", "jcr:primaryType", "jcr:created", "jcr:createdBy", "jcr:uuid"});

    protected Boolean dryRunObject;

    @Override
    public ContainerPipe getParent() {
        return parent;
    }

    @Override
    public void setParent(ContainerPipe parent) {
        this.parent = parent;
    }

    @Override
    public Resource getResource() {
        return resource;
    }

    protected Plumber plumber;

    private String name;

    /**
     * Pipe Constructor
     * @param plumber plumber
     * @param resource configuration resource
     * @param upperBindings already set bindings, can be null
     * @throws Exception in case configuration is not working
     */
    public BasePipe(Plumber plumber, Resource resource, PipeBindings upperBindings) throws Exception {
        this.resource = resource;
        properties = resource.adaptTo(ValueMap.class);
        resolver = resource.getResourceResolver();
        this.plumber = plumber;
        name = properties.get(PN_NAME, resource.getName());
        distributionAgent = properties.get(PN_DISTRIBUTION_AGENT, String.class);
        bindings = upperBindings == null ? new PipeBindings(resource) : upperBindings;
    }

    @Override
    public boolean isDryRun() {
        if (dryRunObject == null) {
            dryRunObject = false;
            try {
                Object run = bindings.isBindingDefined(DRYRUN_KEY) ? bindings.instantiateObject(DRYRUN_EXPR) : false;
                if (run != null) {
                    dryRunObject = true;
                    if (run instanceof Boolean) {
                        dryRunObject = (Boolean) run;
                    } else if (run instanceof String && String.format("%s", Boolean.FALSE).equals(run)) {
                        dryRunObject = false;
                    }
                }
            } catch (ScriptException e){
                logger.error("error evaluating {}, assuming dry run", DRYRUN_EXPR, e);
            }
        }
        return dryRunObject;
    }

    @Override
    public String toString() {
        return name + " " + "(path: " + resource.getPath() + ", dryRun: " + isDryRun() + ", modifiesContent: " + modifiesContent() + ")";
    }

    @Override
    public boolean modifiesContent() {
        return false;
    }

    @Override
    public String getName(){
        return name;
    }

    /**
     * Get pipe's expression, instanciated or not
     * @return configured expression
     */
    public String getExpr() throws ScriptException {
        String rawExpression = properties.get(PN_EXPR, "");
        return bindings.instantiateExpression(rawExpression);
    }

    /**
     * @return configured input path (not computed)
     */
    protected String getRawPath() {
        return properties.get(PN_PATH, "");
    }

    /**
     * Get pipe's path, instanciated or not
     * @return configured path (can be empty)
     */
    public String getPath() throws ScriptException {
        String rawPath = getRawPath();
        return bindings.instantiateExpression(rawPath);
    }

    /**
     * @return computed path: getPath, with relative path taken in account
     * @throws ScriptException
     */
    protected String getComputedPath() throws ScriptException {
        String path = getPath();
        if (StringUtils.isNotBlank(path)) {
            if (!isRootPath(path) && getPreviousResource() != null) {
                path = getPreviousResource().getPath() + SLASH + path;
            }
        }
        return path;
    }

    /**
     * @param path path to be checked
     * @return true if path is root (aka not relative)
     */
    protected boolean isRootPath(String path){
        return path.startsWith(SLASH);
    }

    /**
     * Retrieves previous pipe if contained by a parent, or referrer's
     * @return pipe before this one or the referrer's can be null in case there is no parent
     */
    protected Pipe getPreviousPipe(){
        return referrer == null ? (parent != null ? parent.getPreviousPipe(this) : null) : referrer.getPreviousPipe();
    }

    /**
     * @return previous pipe's output if in a container, null otherwise
     */
    protected Resource getPreviousResource(){
        if (parent != null){
            Pipe previousPipe = getPreviousPipe();
            if (previousPipe != null) {
                return bindings.getExecutedResource(previousPipe.getName());
            }
        }
        return null;
    }

    @Override
    public Resource getInput() throws ScriptException {
        String path = getComputedPath();
        Resource input = null;
        if (StringUtils.isNotBlank(path)){
            input = resolver.getResource(path);
            if (input == null) {
                logger.warn("configured path {} is not found, expect some troubles...", path);
            }
        } else {
            //no input has been configured: we explicitly expect input to come from previouse resource
            input = getPreviousResource();
            if (input == null) {
                logger.warn("no path has been configured, and no previous resource to bind on, expect some troubles...");
            }
        }
        logger.debug("input for this pipe is {}", input != null ? input.getPath() : null);
        return input;
    }

    @Override
    public Object getOutputBinding() {
        if (parent != null){
            Resource output = bindings.getExecutedResource(getName());
            if (output != null) {
                return output.adaptTo(ValueMap.class);
            }
        }
        return null;
    }

    @Override
    public PipeBindings getBindings() {
        return bindings;
    }

    /**
     * default execution, just returns current resource
     * @return output of this pipe, which is here the input resource
     */
    public Iterator<Resource> getOutput() {
        try {
            return computeOutput();
        } catch (Exception e){
            String path = getRawPath();
            if (StringUtils.isBlank(path)){
                Resource input = getPreviousResource();
                if (input != null){
                    path = resource.getPath();
                }
            }
            bindings.setCurrentError(path);
            logger.error("error with pipe execution from {}", path, e);
        }
        return EMPTY_ITERATOR;
    }

    /**
     *
     * @return
     * @throws ScriptException
     */
    protected Iterator<Resource> computeOutput() throws Exception {
        Resource input = getInput();
        if (input != null) {
            return Collections.singleton(input).iterator();
        }
        return EMPTY_ITERATOR;
    }

    /**
     * Get configuration node
     * @return configuration node if any
     */
    public Resource getConfiguration() {
        return resource.getChild(NN_CONF);
    }

    @Override
    public String getDistributionAgent() {
        return distributionAgent;
    }

    @Override
    public void setReferrer(ReferencePipe pipe) {
        referrer = pipe;
    }

    /**
     * Empty resource iterator
     */
    public static final Iterator<Resource> EMPTY_ITERATOR = Collections.emptyIterator();
}
