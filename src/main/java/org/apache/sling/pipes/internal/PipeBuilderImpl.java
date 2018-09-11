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
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.OutputWriter;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.pipes.PipeBuilder;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.pipes.internal.inputstream.CsvPipe;
import org.apache.sling.pipes.internal.inputstream.JsonPipe;
import org.apache.sling.pipes.internal.inputstream.RegexpPipe;
import org.apache.sling.pipes.internal.slingquery.ChildrenPipe;
import org.apache.sling.pipes.internal.slingquery.ClosestPipe;
import org.apache.sling.pipes.internal.slingquery.FindPipe;
import org.apache.sling.pipes.internal.slingquery.ParentPipe;
import org.apache.sling.pipes.internal.slingquery.ParentsPipe;
import org.apache.sling.pipes.internal.slingquery.SiblingsPipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.sling.jcr.resource.JcrResourceConstants.NT_SLING_FOLDER;
import static org.apache.sling.jcr.resource.JcrResourceConstants.NT_SLING_ORDERED_FOLDER;
import static org.apache.sling.jcr.resource.JcrResourceConstants.SLING_RESOURCE_TYPE_PROPERTY;
import static org.apache.sling.pipes.internal.ManifoldPipe.PN_NUM_THREADS;

import static org.apache.sling.pipes.internal.CommandUtil.checkArguments;
import static org.apache.sling.pipes.internal.CommandUtil.writeToMap;
/**
 * Implementation of the PipeBuilder interface
 */
public class PipeBuilderImpl implements PipeBuilder {
    private static final Logger logger = LoggerFactory.getLogger(PipeBuilderImpl.class);

    public static final String PIPES_REPOSITORY_PATH = "/var/pipes";

    public static final String[] DEFAULT_NAMES = new String[]{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"};

    List<Step> steps;

    Map outputs;

    Step containerStep = new Step(ContainerPipe.RESOURCE_TYPE);

    Step currentStep = containerStep;

    Plumber plumber;

    ResourceResolver resolver;

    /**
     * protected constructor (to only allow internal classes to build it out)
     * @param resolver resolver with which the pipe will be built and executed
     * @param plumber instance of the plumber
     */
    protected PipeBuilderImpl(ResourceResolver resolver, Plumber plumber){
        this.plumber = plumber;
        this.resolver = resolver;
    }

    @Override
    public PipeBuilder pipe(String type){
        if (!plumber.isTypeRegistered(type)){
            throw new IllegalArgumentException(type + " is not a registered pipe type");
        }
        if (steps == null){
            steps = new ArrayList<>();
        }
        currentStep = new Step(type);
        steps.add(currentStep);
        return this;
    }

    /**
     * internal utility to glob pipe configuration &amp; expression configuration
     * @param type pipe type
     * @param expr expression
     * @return updated instance of PipeBuilder
     */
    protected PipeBuilder pipeWithExpr(String type, String expr){
        try {
            pipe(type).expr(expr);
        } catch (IllegalAccessException e){
            logger.error("exception while configuring {}", type, e);
        }
        return this;
    }

    @Override
    public PipeBuilder mv(String expr) {
        return pipeWithExpr(MovePipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder write(Object... conf) throws IllegalAccessException {
        return pipe(WritePipe.RESOURCE_TYPE).conf(conf);
    }

    @Override
    public PipeBuilder grep(Object... conf) throws IllegalAccessException {
        return pipe(FilterPipe.RESOURCE_TYPE).conf(conf);
    }

    @Override
    public PipeBuilder auth(Object... conf) throws IllegalAccessException {
        return pipe(AuthorizablePipe.RESOURCE_TYPE).conf(conf);
    }

    @Override
    public PipeBuilder xpath(String expr) {
        return pipeWithExpr(XPathPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder children(String expr) {
        return pipeWithExpr(ChildrenPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder rm() {
        return pipe(RemovePipe.RESOURCE_TYPE);
    }

    @Override
    public PipeBuilder traverse() {
        return pipe(TraversePipe.RESOURCE_TYPE);
    }


    @Override
    public PipeBuilder csv(String expr) {
        return pipeWithExpr(CsvPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder json(String expr) {
        return pipeWithExpr(JsonPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder egrep(String expr) {
        return pipeWithExpr(RegexpPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder mkdir(String expr) {
        return pipeWithExpr(PathPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder echo(String path) {
        try {
            pipe(BasePipe.RESOURCE_TYPE).path(path);
        } catch(IllegalAccessException e){
            logger.error("error when calling echo {}", path, e);
        }
        return this;
    }

    @Override
    public PipeBuilder parent() {
        return pipe(ParentPipe.RESOURCE_TYPE);
    }

    @Override
    public PipeBuilder parents(String expr) {
        return pipeWithExpr(ParentsPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder siblings(String expr) {
        return pipeWithExpr(SiblingsPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder closest(String expr) {
        return pipeWithExpr(ClosestPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder $(String expr) {
        return pipeWithExpr(FindPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder ref(String expr) {
        return pipeWithExpr(ReferencePipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder mp() {
        return pipe(MultiPropertyPipe.RESOURCE_TYPE);
    }

    @Override
    public PipeBuilder pkg(String expr) {
        try {
            pipeWithExpr(PackagePipe.RESOURCE_TYPE, expr).with(PackagePipe.PN_FILTERCOLLECTIONMODE, true);
        } catch (IllegalAccessException e) {
            logger.error("error when calling pkg", e);
        }
        return this;
    }

    @Override
    public PipeBuilder not(String expr) {
        return pipeWithExpr(NotPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder with(Object... params) throws IllegalAccessException {
        return writeToCurrentStep(null, params);
    }

    @Override
    public PipeBuilder conf(Object... properties) throws IllegalAccessException {
        return writeToCurrentStep(Pipe.NN_CONF, properties);
    }

    /**
     * Add some configurations to current's Step node defined by name (if null, will be step's properties)
     * @param name name of the configuration node, can be null in which case it's the subpipe itself
     * @param params key/value pair list of configuration
     * @return updated instance of PipeBuilder
     * @throws IllegalAccessException in case configuration is wrong
     */
    protected PipeBuilder writeToCurrentStep(String name, Object... params) throws IllegalAccessException {
        checkArguments(params);
        Map props = name != null ? currentStep.confs.get(name) : currentStep.properties;
        if (props == null){
            props = new HashMap();
            if (name != null){
                currentStep.confs.put(name, props);
            }
        }
        writeToMap(props, params);
        return this;
    }

    @Override
    public PipeBuilder expr(String value) throws IllegalAccessException {
        return this.with(Pipe.PN_EXPR, value);
    }

    @Override
    public PipeBuilder path(String value) throws IllegalAccessException {
        return this.with(Pipe.PN_PATH, value);
    }

    @Override
    public PipeBuilder name(String name) throws IllegalAccessException {
        currentStep.name = name;
        return this;
    }

    /**
     * build a time + random based path under /var/pipes
     * @return full path of future Pipe
     */
    protected String buildRandomPipePath() {
        final Calendar now = Calendar.getInstance();
        return PIPES_REPOSITORY_PATH + '/' + now.get(Calendar.YEAR) + '/' + now.get(Calendar.MONTH) + '/' + now.get(Calendar.DAY_OF_MONTH) + "/"
                + UUID.randomUUID().toString();
    }

    /**
     * Create a configuration resource
     * @param resolver current resolver
     * @param path path of the resource
     * @param type type of the node to be created
     * @param data map of properties to add
     * @throws PersistenceException in case configuration resource couldn't be persisted
     * @return resource created
     */
    protected Resource createResource(ResourceResolver resolver, String path, String type, Map data) throws PersistenceException {
        return ResourceUtil.getOrCreateResource(resolver, path, data, type, false);
    }

    @Override
    public PipeBuilder outputs(String... keys) {
        outputs = new HashMap();
        writeToMap(outputs, keys);
        return this;
    }

    @Override
    public Pipe build() throws PersistenceException {
        return build(buildRandomPipePath());
    }

    /**
     * Persist a step at a given path
     * @param path path into which step should be persisted
     * @param parentType type of the parent resource
     * @param step step to persist
     * @return created resource
     * @throws PersistenceException in case persistence fails
     */
    protected Resource persistStep(String path, String parentType, Step step) throws PersistenceException {
        Resource resource = createResource(resolver, path, parentType, step.properties);
        for (Map.Entry<String, Map> entry : step.confs.entrySet()){
            createResource(resolver, path + "/" + entry.getKey(), NT_SLING_FOLDER, entry.getValue());
            logger.debug("built pipe {}'s {} node", path, entry.getKey());
        }
        return resource;
    }

    @Override
    public Pipe build(String path) throws PersistenceException {
        Resource pipeResource = persistStep(path, NT_SLING_FOLDER, containerStep);
        if (outputs != null){
            ResourceUtil.getOrCreateResource(resolver, path + "/" + OutputWriter.PARAM_WRITER, outputs, NT_SLING_FOLDER, false);
        }
        int index = 0;
        for (Step step : steps){
            String name = StringUtils.isNotBlank(step.name) ? step.name : DEFAULT_NAMES.length > index ? DEFAULT_NAMES[index] : Integer.toString(index);
            index++;
            persistStep(path + "/" + Pipe.NN_CONF + "/" + name, NT_SLING_ORDERED_FOLDER, step);
        }
        resolver.commit();
        logger.debug("built pipe under {}", path);
        return plumber.getPipe(pipeResource);
    }

    @Override
    public ExecutionResult run() throws Exception {
        return run(null);
    }

    @Override
    public ExecutionResult runWith(Object... bindings) throws Exception {
        checkArguments(bindings);
        Map bindingsMap = new HashMap();
        writeToMap(bindingsMap, bindings);
        return run(bindingsMap);
    }

    @Override
    public ExecutionResult run(Map bindings) throws Exception {
        JsonWriter writer = new JsonWriter();
        writer.starts();
        Pipe pipe = this.build();
        return plumber.execute(resolver, pipe, bindings,  writer , true);
    }

    @Override
    public Job runAsync(Map bindings) throws PersistenceException {
        Pipe pipe = this.build();
        return plumber.executeAsync(resolver, pipe.getResource().getPath(), bindings);
    }

    @Override
    public ExecutionResult runParallel(int numThreads, Map additionalBindings) throws Exception {
        containerStep.setType(ManifoldPipe.RESOURCE_TYPE);
        Map bindings = new HashMap() {{put(PN_NUM_THREADS, numThreads);}};
        if (additionalBindings != null){
            bindings.putAll(additionalBindings);
        }
        return run(bindings);
    }

    /**
     * holds a subpipe set of informations
     */
    public class Step {
        String name;
        Map properties;
        Map<String, Map> confs = new HashMap<>();
        Step(String type){
            properties = new HashMap();
            setType(type);
        }

        void setType(String type){
            properties.put(SLING_RESOURCE_TYPE_PROPERTY, type);
        }
    }
}