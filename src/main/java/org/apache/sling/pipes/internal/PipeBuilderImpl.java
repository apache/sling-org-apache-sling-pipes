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
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.OutputWriter;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.PipeBuilder;
import org.apache.sling.pipes.PipeExecutor;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.sling.jcr.resource.JcrResourceConstants.NT_SLING_FOLDER;
import static org.apache.sling.jcr.resource.JcrResourceConstants.NT_SLING_ORDERED_FOLDER;
import static org.apache.sling.jcr.resource.JcrResourceConstants.SLING_RESOURCE_TYPE_PROPERTY;
import static org.apache.sling.pipes.BasePipe.SLASH;
import static org.apache.sling.pipes.CommandUtil.checkArguments;
import static org.apache.sling.pipes.CommandUtil.writeToMap;
import static org.apache.sling.pipes.internal.ManifoldPipe.PN_NUM_THREADS;
/**
 * Implementation of the PipeBuilder interface
 */
public class PipeBuilderImpl implements PipeBuilder {
    private static final Logger logger = LoggerFactory.getLogger(PipeBuilderImpl.class);

    private static final String[] DEFAULT_NAMES = new String[]{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"};

    List<Step> steps;

    Map<String, Object> outputs;

    Step containerStep = new Step(ContainerPipe.RESOURCE_TYPE);

    Step currentStep = containerStep;

    Plumber plumber;

    ResourceResolver resolver;

    /**
     * constructor (to only allow internal classes to build it out)
     * @param resolver resolver with which the pipe will be built and executed
     * @param plumber instance of the plumber
     */
    PipeBuilderImpl(ResourceResolver resolver, Plumber plumber){
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
     * @return updated instance of PipeBuilder
     */
    PipeBuilder pipeWithExpr(String type, String expr){
        try {
            pipe(type).expr(expr);
        } catch (IllegalAccessException e){
            logger.error("exception while configuring {}", type, e);
        }
        return this;
    }

    @Override
    @PipeExecutor(command = "mv", resourceType = MovePipe.RESOURCE_TYPE, pipeClass = MovePipe.class,
            description = "move current resource to expr (more on https://sling.apache.org/documentation/bundles/sling-pipes/writers.html)")
    public PipeBuilder mv(String expr) {
        return pipeWithExpr(MovePipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "write", resourceType = WritePipe.RESOURCE_TYPE, pipeClass = WritePipe.class,
            description = "write following key=value pairs to the current resource")
    public PipeBuilder write(Object... conf) throws IllegalAccessException {
        PipeBuilder instance = pipe(WritePipe.RESOURCE_TYPE);
        if (conf.length > 0) {
            instance = instance.conf(conf);
        }
        return instance;
    }

    @Override
    @PipeExecutor(command = "grep", resourceType = FilterPipe.RESOURCE_TYPE, pipeClass = FilterPipe.class,
            description = "filter current resources with following key=value pairs")
    public PipeBuilder grep(Object... conf) throws IllegalAccessException {
        return pipe(FilterPipe.RESOURCE_TYPE).conf(conf);
    }

    @Override
    @PipeExecutor(command = "auth", resourceType = AuthorizablePipe.RESOURCE_TYPE, pipeClass = AuthorizablePipe.class,
            description = "convert current resource as authorizable")
    public PipeBuilder auth(Object... conf) throws IllegalAccessException {
        return pipe(AuthorizablePipe.RESOURCE_TYPE).conf(conf);
    }

    @Override
    @PipeExecutor(command = "xpath", resourceType = XPathPipe.RESOURCE_TYPE, pipeClass = XPathPipe.class,
            description = "create following xpath query's result as output resources")
    public PipeBuilder xpath(String expr) {
        return pipeWithExpr(XPathPipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "children", resourceType = ChildrenPipe.RESOURCE_TYPE, pipeClass = ChildrenPipe.class,
            description = "list current resource's immediate children")
    public PipeBuilder children(String expr) {
        return pipeWithExpr(ChildrenPipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "rm", resourceType = RemovePipe.RESOURCE_TYPE, pipeClass =  RemovePipe.class,
            description = "remove current resource")
    public PipeBuilder rm() {
        return pipe(RemovePipe.RESOURCE_TYPE);
    }

    @Override
    @PipeExecutor(command = "traverse", resourceType = TraversePipe.RESOURCE_TYPE, pipeClass = TraversePipe.class,
            description = "traverse current resource")
    public PipeBuilder traverse() {
        return pipe(TraversePipe.RESOURCE_TYPE);
    }


    @Override
    @PipeExecutor(command = "csv", resourceType = CsvPipe.RESOURCE_TYPE, pipeClass = CsvPipe.class,
            description = "read expr's csv and output each line in the bindings")
    public PipeBuilder csv(String expr) {
        return pipeWithExpr(CsvPipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "json", resourceType = JsonPipe.RESOURCE_TYPE, pipeClass = JsonPipe.class,
            description = "read expr's json array and output each object in the bindings")
    public PipeBuilder json(String expr) {
        return pipeWithExpr(JsonPipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "egrep", resourceType = RegexpPipe.RESOURCE_TYPE, pipeClass = RegexpPipe.class,
            description = "read expr's txt and output each found pattern in the binding")
    public PipeBuilder egrep(String expr) {
        return pipeWithExpr(RegexpPipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "mkdir", resourceType = PathPipe.RESOURCE_TYPE, pipeClass = PathPipe.class,
            description = "create expr path")
    public PipeBuilder mkdir(String expr) {
        return pipeWithExpr(PathPipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "echo", resourceType = BasePipe.RESOURCE_TYPE, pipeClass = BasePipe.class,
            description = "output input's path")
    public PipeBuilder echo(String path) {
        try {
            pipe(BasePipe.RESOURCE_TYPE).path(path);
        } catch(IllegalAccessException e){
            logger.error("error when calling echo {}", path, e);
        }
        return this;
    }

    @Override
    @PipeExecutor(command = "parent", resourceType = ParentPipe.RESOURCE_TYPE, pipeClass = ParentPipe.class,
            description = "return current's resource parent")
    public PipeBuilder parent() {
        return pipe(ParentPipe.RESOURCE_TYPE);
    }

    @Override
    @PipeExecutor(command = "parents", resourceType = ParentsPipe.RESOURCE_TYPE, pipeClass = ParentsPipe.class,
            description = "return current's resource parents")
    public PipeBuilder parents(String expr) {
        return pipeWithExpr(ParentsPipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "siblings", resourceType = SiblingsPipe.RESOURCE_TYPE, pipeClass = SiblingsPipe.class,
            description = "list current resource's siblings")
    public PipeBuilder siblings(String expr) {
        return pipeWithExpr(SiblingsPipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "closest", resourceType = ClosestPipe.RESOURCE_TYPE, pipeClass = ClosestPipe.class,
            description = "return closest resource of the current")
    public PipeBuilder closest(String expr) {
        return pipeWithExpr(ClosestPipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "$", resourceType = FindPipe.RESOURCE_TYPE, pipeClass = FindPipe.class,
            description = "find resource from the current, with the given expression as a parameter")
    public PipeBuilder $(String expr) {
        return pipeWithExpr(FindPipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "ref", resourceType = ReferencePipe.RESOURCE_TYPE, pipeClass = ReferencePipe.class,
            description = "reference passed pipe")
    public PipeBuilder ref(String expr) {
        return pipeWithExpr(ReferencePipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "shallowRef", resourceType = ShallowReferencePipe.RESOURCE_TYPE, pipeClass = ShallowReferencePipe.class,
            description = "shallow reference passed pipe, to be used for recursive usage")
    public PipeBuilder shallowRef(String expr) {
        return pipeWithExpr(ShallowReferencePipe.RESOURCE_TYPE, expr);
    }

    @Override
    @PipeExecutor(command = "mp", resourceType = MultiPropertyPipe.RESOURCE_TYPE, pipeClass = MultiPropertyPipe.class,
            description = "read MULTI property, and output each value in the bindings")
    public PipeBuilder mp() {
        return pipe(MultiPropertyPipe.RESOURCE_TYPE);
    }

    @Override
    @PipeExecutor(command = "pkg", resourceType = PackagePipe.RESOURCE_TYPE, pipeClass = PackagePipe.class,
            description = "package up current resource in given package")
    public PipeBuilder pkg(String expr) {
        try {
            pipeWithExpr(PackagePipe.RESOURCE_TYPE, expr).with(PackagePipe.PN_FILTERCOLLECTIONMODE, true);
        } catch (IllegalAccessException e) {
            logger.error("error when calling pkg", e);
        }
        return this;
    }

    @Override
    @PipeExecutor(command = "not", resourceType = NotPipe.RESOURCE_TYPE, pipeClass = NotPipe.class,
            description = "invert output: if input, return nothing, if no input, return single resource")
    public PipeBuilder not(String expr) {
        return pipeWithExpr(NotPipe.RESOURCE_TYPE, expr);
    }

    @Override
    public PipeBuilder with(Object... params) throws IllegalAccessException {
        return writeToCurrentStep(null, true, params);
    }

    @Override
    public PipeBuilder withStrings(String... params){
        return writeToCurrentStep(null, false, params);
    }

    @Override
    public PipeBuilder conf(Object... properties) throws IllegalAccessException {
        return writeToCurrentStep(Pipe.NN_CONF, true, properties);
    }

    @Override
    public PipeBuilder bindings(Object... bindings) throws IllegalAccessException {
        return writeToCurrentStep(PipeBindings.NN_ADDITIONALBINDINGS, true, bindings);
    }

    @Override
    @PipeExecutor(command = "acls", resourceType = ACLPipe.RESOURCE_TYPE, pipeClass = ACLPipe.class,
            description = "output each acls on the resource or  acls for authorizable in repository in bindings")
    public PipeBuilder acls() {
        return pipe(ACLPipe.RESOURCE_TYPE);
    }

    @Override
    @PipeExecutor(command = "allow", resourceType = ACLPipe.RESOURCE_TYPE, pipeClass = ACLPipe.class,
            description = "sets allow acls on the resource")
    public PipeBuilder allow(String expr) throws IllegalAccessException{
        return pipeWithExpr(ACLPipe.RESOURCE_TYPE, expr).with("allow","true");
    }

    @Override
    @PipeExecutor(command = "deny", resourceType = ACLPipe.RESOURCE_TYPE, pipeClass = ACLPipe.class,
            description = "sets deny acls on the resource")
    public PipeBuilder deny(String expr) throws IllegalAccessException{
        return pipeWithExpr(ACLPipe.RESOURCE_TYPE, expr).with("deny","true");
    }

    /**
     * Add some configurations to current's Step node defined by name (if null, will be step's properties)
     * @param name name of the configuration node, can be null in which case it's the subpipe itself
     * @param embed indicates wether or not we should try to embed values in script tags
     * @param params key/value pair list of configuration
     * @return updated instance of PipeBuilder
     */
    PipeBuilder writeToCurrentStep (String name, boolean embed, Object... params) {
        checkArguments(params);
        Map<String, Object> props = name != null ? currentStep.confs.get(name) : currentStep.properties;
        if (props == null) {
            props = new HashMap<>();
            if (name != null) {
                currentStep.confs.put(name, props);
            }
        }
        writeToMap(props, embed, params);
        return this;
    }

    @Override
    public PipeBuilder expr(String value) throws IllegalAccessException {
        return this.withStrings(Pipe.PN_EXPR, value);
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
     * Create a configuration resource
     * @param resolver current resolver
     * @param path path of the resource
     * @param type type of the node to be created
     * @param data map of properties to add
     * @throws PersistenceException in case configuration resource couldn't be persisted
     * @return resource created
     */
    Resource createResource(ResourceResolver resolver, String path, String type, Map<String, Object> data) throws PersistenceException {
        if (data.keySet().stream().noneMatch(k -> k.contains(SLASH))) {
            return ResourceUtil.getOrCreateResource(resolver, path, data, type, false);
        }
        String returnPath = EMPTY;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (entry.getKey().contains(SLASH)) {
                String deepPath = String.join(SLASH, path, StringUtils.substringBeforeLast(entry.getKey(), SLASH));
                createResource(resolver, deepPath, type, Collections.singletonMap(
                    StringUtils.substringAfterLast(entry.getKey(), SLASH), entry.getValue()));
                if (returnPath.isEmpty() || returnPath.length() > deepPath.length()) {
                    returnPath = deepPath;
                }
            }
        }
        return resolver.getResource(returnPath);
    }

    @Override
    public PipeBuilder outputs(String... keys) {
        outputs = new HashMap<>();
        writeToMap(outputs, true, keys);
        return this;
    }

    @Override
    public Pipe build() throws PersistenceException {
        return build(plumber.generateUniquePath());
    }

    /**
     * Persist a step at a given path
     * @param path path into which step should be persisted
     * @param parentType type of the parent resource
     * @param step step to persist
     * @return created resource
     * @throws PersistenceException in case persistence fails
     */
    Resource persistStep(String path, String parentType, Step step) throws PersistenceException {
        Resource resource = createResource(resolver, path, parentType, step.properties);
        ModifiableValueMap mvm = resource.adaptTo(ModifiableValueMap.class);
        if (StringUtils.isNotBlank(step.name) && mvm != null){
            mvm.put(Pipe.PN_NAME, step.name);
        }
        for (Map.Entry<String, Map<String, Object>> entry : step.confs.entrySet()){
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
            String name = DEFAULT_NAMES.length > index ? DEFAULT_NAMES[index] : Integer.toString(index);
            if (StringUtils.isNotBlank(step.name)) {
                name = step.name;
            }
            index++;
            persistStep(path + "/" + Pipe.NN_CONF + "/" + name, NT_SLING_ORDERED_FOLDER, step);
        }
        resolver.commit();
        logger.debug("built pipe under {}", path);
        return plumber.getPipe(pipeResource);
    }

    @Override
    public ExecutionResult run() {
        return run(null);
    }

    @Override
    public ExecutionResult runWith(Object... bindings) {
        checkArguments(bindings);
        Map<String, Object> bindingsMap = new HashMap<>();
        writeToMap(bindingsMap, true, bindings);
        return run(bindingsMap);
    }

    @Override
    public ExecutionResult run(Map<String, Object> bindings) {
        JsonWriter writer = new JsonWriter();
        try {
            writer.starts();
            Pipe pipe = this.build();
            return plumber.execute(resolver, pipe, bindings, writer, true);
        } catch (PersistenceException e) {
            logger.error("unable to build the pipe", e);
        }
        return new ExecutionResult(writer);
    }

    @Override
    public Job runAsync(Map<String, Object> bindings) throws PersistenceException {
        Pipe pipe = this.build();
        return plumber.executeAsync(resolver, pipe.getResource().getPath(), bindings);
    }

    @Override
    public ExecutionResult runParallel(int numThreads, Map<String, Object> additionalBindings) {
        containerStep.setType(ManifoldPipe.RESOURCE_TYPE);
        Map<String, Object> bindings = new HashMap<>();
        bindings.put(PN_NUM_THREADS, numThreads);
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
        Map<String, Object> bindings;
        Map<String, Object> properties;
        Map<String, Map<String, Object>> confs = new HashMap<>();
        Step(String type){
            properties = new HashMap<>();
            setType(type);
        }

        void setType(String type){
            properties.put(SLING_RESOURCE_TYPE_PROPERTY, type);
        }
    }
}