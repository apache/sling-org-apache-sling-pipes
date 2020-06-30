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
import org.apache.sling.pipes.internal.BindingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.sling.pipes.PipeBindings.NN_ADDITIONALBINDINGS;
import static org.apache.sling.pipes.PipeBindings.NN_PROVIDERS;
import static org.apache.sling.pipes.PipeBindings.PN_ADDITIONALSCRIPTS;

/**
 * provides generic utilities for a pipe, is also a dummy pipe (outputs its input, without changing anything)
 */
public class BasePipe implements Pipe {

    private final Logger logger = LoggerFactory.getLogger(BasePipe.class);
    public static final String SLASH = "/";
    public static final String RT_PREFIX = "slingPipes/";
    public static final String RESOURCE_TYPE = RT_PREFIX + "base";
    public static final String DRYRUN_KEY = "dryRun";
    public static final String DRYRUN_EXPR = "${" + DRYRUN_KEY + "}";
    public static final String READ_ONLY = "readOnly";
    public static final String PN_STATUS = "status";
    public static final String PN_STATUS_MODIFIED = "statusModified";
    public static final String PN_BEFOREHOOK = "beforeHook";
    public static final String PN_AFTERHOOK = "afterHook";
    public static final String STATUS_STARTED = "started";
    public static final String STATUS_FINISHED = "finished";

    protected ResourceResolver resolver;
    protected ValueMap properties;
    protected Resource resource;
    protected SuperPipe parent;
    protected String distributionAgent;
    protected PipeBindings bindings;
    protected List<BindingProvider> bindingProviders;

    protected String beforeHook;
    protected String afterHook;

    // used by pipes using complex JCR configurations
    public static final List<String> IGNORED_PROPERTIES = Arrays.asList(new String[]{"jcr:lastModified", "jcr:primaryType", "jcr:created", "jcr:createdBy", "jcr:uuid"});

    protected Boolean dryRunObject;

    @Override
    public SuperPipe getParent() {
        return parent;
    }

    @Override
    public void setParent(SuperPipe parent) {
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
        beforeHook = properties.get(PN_BEFOREHOOK, String.class);
        afterHook = properties.get(PN_AFTERHOOK, String.class);
        extractAdditionalBindings(resource, upperBindings);
    }

    /**
     * because additional bindings or scripts can be defined at a subpipe level
     * @param resource
     * @param upperBindings
     * @throws ScriptException
     */
    private void extractAdditionalBindings(Resource resource, PipeBindings upperBindings) throws ScriptException {
        bindings = upperBindings == null ? new PipeBindings(resource) : upperBindings;
        //additional bindings (global variables to use in child pipes expressions)
        Resource additionalBindings = resource.getChild(NN_ADDITIONALBINDINGS);
        if (additionalBindings != null) {
            logger.debug("additional bindings are detected");
            ValueMap additionalMap = additionalBindings.adaptTo(ValueMap.class);
            bindings.addBindings(additionalMap);
            for (String ignoredProperty : BasePipe.IGNORED_PROPERTIES){
                bindings.getBindings().remove(ignoredProperty);
            }
            Resource providers = additionalBindings.getChild(NN_PROVIDERS);
            if (providers != null){
                logger.debug("bindings provider are detected");
                bindingProviders = new ArrayList<>();
                for (Resource provider : providers.getChildren()){
                    Pipe pipe = plumber.getPipe(provider, bindings);
                    if (pipe == null) {
                        logger.error("pipe provided in {} is not correct", provider.getPath());
                    } else if (pipe.modifiesContent()) {
                        logger.error("content modifiers like {} are not usable as binding providers", provider.getPath());
                    } else {
                        bindingProviders.add(new BindingProvider(pipe));
                    }
                }
            }
        }
        Resource scriptsResource = resource.getChild(PN_ADDITIONALSCRIPTS);
        if (scriptsResource != null) {
            String[] scripts = scriptsResource.adaptTo(String[].class);
            if (scripts != null) {
                for (String script : scripts){
                    bindings.addScript(resource.getResourceResolver(), script);
                }
            }
        }
        bindings.addBinding(getName(), EMPTY);
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
     * @return configured expression (not computed)
     */
    public String getRawExpression() {
        return properties.get(PN_EXPR, "");
    }

    /**
     * Get pipe's expression, instanciated or not
     * @return configured expression
     * @throws ScriptException in case computed expression goes wrong
     */
    public String getExpr() throws ScriptException {
        return bindings.instantiateExpression(getRawExpression());
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
     * @throws ScriptException in case computed path goes wrong
     */
    public String getPath() throws ScriptException {
        String rawPath = getRawPath();
        return bindings.instantiateExpression(rawPath);
    }

    /**
     * @return computed path: getPath, with relative path taken in account
     * @throws ScriptException in case computed path goes wrong
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
        return parent != null ? parent.getPreviousPipe(this) : null;
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
                logger.warn("configured path {} for {} is not found, expect some troubles...", path, getName());
            }
        } else {
            //no input has been configured: we explicitly expect input to come from previous resource
            input = getPreviousResource();
            if (input == null) {
                logger.warn("no valid path has been configured for {}, and no previous resource to bind on, expect some troubles...",
                    getName());
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
     * will execute in parallel binding providers if any, and updated Pipe bindings with returned values
     */
    protected void provideAdditionalBindings(){
        if (bindingProviders != null && bindingProviders.size() > 0){
            try {
                ExecutorService executor = Executors.newWorkStealingPool();
                List<Future<ValueMap>> additionalBindings = executor.invokeAll(bindingProviders);
                for (Future<ValueMap> additionalBinding : additionalBindings){
                    ValueMap binding = additionalBinding.get();
                    Pipe pipe = bindingProviders.get(additionalBindings.indexOf(additionalBinding)).getPipe();
                    logger.debug("adding binding " +
                            "{}={}", pipe.getName(), binding);
                    bindings.addBinding(pipe.getName(), binding);
                }
            } catch (InterruptedException | ExecutionException e){
                logger.error("unable to provide additional bindings", e);
            }
        }
    }

    /**
     * default execution, just returns current resource
     * @return output of this pipe, which is here the input resource
     */
    public Iterator<Resource> getOutput() {
        try {
            provideAdditionalBindings();
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

    @Override
    public void before() throws Exception {
        if (StringUtils.isNotBlank(beforeHook)){
            plumber.newPipe(resolver).ref(beforeHook).run();
        }
    }

    @Override
    public void after() throws Exception {
        if (StringUtils.isNotBlank(afterHook)){
            plumber.newPipe(resolver).ref(afterHook).run();
        }
    }

    /**
     * @return outputs of the pipe, as an iterator of resources
     * @throws ScriptException if any exception has occured
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

    /**
     * Empty resource iterator
     */
    public static final Iterator<Resource> EMPTY_ITERATOR = Collections.emptyIterator();
}
