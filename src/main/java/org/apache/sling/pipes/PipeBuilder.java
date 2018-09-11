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

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.pipes.internal.AuthorizablePipe;
import org.apache.sling.pipes.internal.FilterPipe;
import org.apache.sling.pipes.internal.MovePipe;
import org.apache.sling.pipes.internal.MultiPropertyPipe;
import org.apache.sling.pipes.internal.NotPipe;
import org.apache.sling.pipes.internal.PackagePipe;
import org.apache.sling.pipes.internal.PathPipe;
import org.apache.sling.pipes.internal.ReferencePipe;
import org.apache.sling.pipes.internal.RemovePipe;
import org.apache.sling.pipes.internal.TraversePipe;
import org.apache.sling.pipes.internal.WritePipe;
import org.apache.sling.pipes.internal.XPathPipe;
import org.apache.sling.pipes.internal.inputstream.CsvPipe;
import org.apache.sling.pipes.internal.inputstream.JsonPipe;
import org.apache.sling.pipes.internal.inputstream.RegexpPipe;
import org.apache.sling.pipes.internal.slingquery.ChildrenPipe;
import org.apache.sling.pipes.internal.slingquery.ClosestPipe;
import org.apache.sling.pipes.internal.slingquery.FindPipe;
import org.apache.sling.pipes.internal.slingquery.ParentPipe;
import org.apache.sling.pipes.internal.slingquery.ParentsPipe;
import org.apache.sling.pipes.internal.slingquery.SiblingsPipe;
import org.osgi.annotation.versioning.ProviderType;

import java.util.Map;

/**
 * Builder and Runner of a pipe, based on a fluent API, for script and java usage.
 */
@ProviderType
public interface PipeBuilder {
    /**
     * attach a new pipe to the current context
     * @param type resource type (should be registered by the plumber)
     * @return updated instance of PipeBuilder
     */
    PipeBuilder pipe(String type);

    /**
     * attach a move pipe to the current context
     * @param expr target of the resource to move
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "mv", resourceType = MovePipe.RESOURCE_TYPE, pipeClass = MovePipe.class,
            description = "move current resource to expr (more on https://sling.apache.org/documentation/bundles/sling-pipes/writers.html)")
    PipeBuilder mv(String expr);

    /**
     * attach a write pipe to the current context
     * @param conf configuration parameters
     * @return updated instance of PipeBuilder
     * @throws IllegalAccessException in case it's called with bad configuration
     */
    @PipeExecutor(command = "write", resourceType = WritePipe.RESOURCE_TYPE, pipeClass = WritePipe.class,
            description = "write following key=value pairs to the current resource")
    PipeBuilder write(Object... conf) throws IllegalAccessException;

    /**
     * attach a filter pipe to the current context
     * @param conf configuration parameters
     * @return updated instance of PipeBuilder
     * @throws IllegalAccessException in case it's called with bad configuration
     */
    @PipeExecutor(command = "grep", resourceType = FilterPipe.RESOURCE_TYPE, pipeClass = FilterPipe.class,
            description = "filter current resources with following key=value pairs")
    PipeBuilder grep(Object... conf) throws IllegalAccessException;

    /**
     * attach an authorizable pipe to the current context
     * @param conf configuration key value pairs for authorizable (see pipe's doc)
     * @return updated instance of PipeBuilder
     * @throws IllegalAccessException in case it's called with bad configuration
     */
    @PipeExecutor(command = "auth", resourceType = AuthorizablePipe.RESOURCE_TYPE, pipeClass = AuthorizablePipe.class,
            description = "convert current resource as authorizable")
    PipeBuilder auth(Object... conf) throws IllegalAccessException;

    /**
     * attach a xpath pipe to the current context
     * @param expr xpath expression
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "xpath", resourceType = XPathPipe.RESOURCE_TYPE, pipeClass = XPathPipe.class,
            description = "create following xpath query's result as output resources")
    PipeBuilder xpath(String expr);

    /**
     * attach a sling query children pipe to the current context
     * @param expr sling query expression
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "children", resourceType = ChildrenPipe.RESOURCE_TYPE, pipeClass = ChildrenPipe.class,
            description = "list current resource's immediate children")
    PipeBuilder children(String expr);

    /**
     * attach a sling query siblings pipe to the current context
     * @param expr sling query expression
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "siblings", resourceType = SiblingsPipe.RESOURCE_TYPE, pipeClass = SiblingsPipe.class,
        description = "list current resource's siblings")
    PipeBuilder siblings(String expr);

    /**
     * attach a rm pipe to the current context
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "rm", resourceType = RemovePipe.RESOURCE_TYPE, pipeClass =  RemovePipe.class,
            description = "remove current resource")
    PipeBuilder rm();

    /**
     * attach a csv pipe to the current context
     * @param expr csv expr or URL or path in the resource tree
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "csv", resourceType = CsvPipe.RESOURCE_TYPE, pipeClass = CsvPipe.class,
        description = "read expr's csv and output each line in the bindings")
    PipeBuilder csv(String expr);

    /**
     * attach a json pipe to the current context
     * @param expr json expr or URL or path in the resource tree
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "json", resourceType = JsonPipe.RESOURCE_TYPE, pipeClass = JsonPipe.class,
            description = "read expr's json array and output each object in the bindings")
    PipeBuilder json(String expr);

    /**
     * attach a Regexp pipe to the current context
     * @param expr text expr or URL or path in the resource tree
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "egrep", resourceType = RegexpPipe.RESOURCE_TYPE, pipeClass = RegexpPipe.class,
            description = "read expr's txt and output each found pattern in the binding")
    PipeBuilder egrep(String expr);

    /**
     * Attach a path pipe to the current context
     * @param expr path to create
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "mkdir", resourceType = PathPipe.RESOURCE_TYPE, pipeClass = PathPipe.class,
            description = "create expr path")
    PipeBuilder mkdir(String expr);

    /**
     * attach a base pipe to the current context
     * @param path pipe path
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "echo", resourceType = BasePipe.RESOURCE_TYPE, pipeClass = BasePipe.class,
            description = "output input's path")
    PipeBuilder echo(String path);

    /**
     * attach a traverse pipe to the current context
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "traverse", resourceType = TraversePipe.RESOURCE_TYPE, pipeClass = TraversePipe.class,
            description = "traverse current resource")
    PipeBuilder traverse();

    /**
     * attach a sling query parent pipe to the current context
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "parent", resourceType = ParentPipe.RESOURCE_TYPE, pipeClass = ParentPipe.class,
            description = "return current's resource parent")
    PipeBuilder parent();

    /**
     * attach a sling query parents pipe to the current context
     * @param expr expression
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "parents", resourceType = ParentsPipe.RESOURCE_TYPE, pipeClass = ParentsPipe.class,
            description = "return current's resource parents")
    PipeBuilder parents(String expr);

    /**
     * attach a sling query closest pipe to the current context
     * @param expr expression
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "closest", resourceType = ClosestPipe.RESOURCE_TYPE, pipeClass = ClosestPipe.class,
            description = "return closest resource of the current")
    PipeBuilder closest(String expr);

    /**
     * attach a sling query find pipe to the current context
     * @param expr expression
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "$", resourceType = FindPipe.RESOURCE_TYPE, pipeClass = FindPipe.class,
            description = "find resource from the current, with the given expression as a parameter")
    PipeBuilder $(String expr);

    /**
     * attach a reference pipe to the current context
     * @param expr reference
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "ref", resourceType = ReferencePipe.RESOURCE_TYPE, pipeClass = ReferencePipe.class,
            description = "reference passed pipe")
    PipeBuilder ref(String expr);

    /**
     * attach a package pipe, in filter collection mode as default
     * @param expr path of the pipe
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "pkg", resourceType = PackagePipe.RESOURCE_TYPE, pipeClass = PackagePipe.class,
            description = "package up current resource in given package")
    PipeBuilder pkg(String expr);

    /**
     * attach a not pipe to the current context
     * @param expr reference
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "not", resourceType = NotPipe.RESOURCE_TYPE, pipeClass = NotPipe.class,
            description = "invert output: if input, return nothing, if no input, return single resource")
    PipeBuilder not(String expr);

    /**
     * attach a multi value property pipe to the current context
     * @return updated instance of PipeBuilder
     */
    @PipeExecutor(command = "mp", resourceType = MultiPropertyPipe.RESOURCE_TYPE, pipeClass = MultiPropertyPipe.class,
            description = "read multi property, and output each value in the bindings")
    PipeBuilder mp();

    /**
     * parameterized current pipe in the context
     * @param params key value pair of parameters
     * @return updated instance of PipeBuilder
     * @throws IllegalAccessException in case it's called with wrong # of arguments
     */
    PipeBuilder with(Object... params) throws IllegalAccessException;

    /**
     * set an expr configuration to the current pipe in the context
     * @param value expression value
     * @return updated instance of PipeBuilder
     * @throws IllegalAccessException in case it's called in a bad time
     */
    PipeBuilder expr(String value) throws IllegalAccessException;

    /**
     * sets a pipe name, important in case you want to reuse it in another expression
     * @param name to overwrite default binding name (otherwise it will be "one", "two", ...)
     * @return updated instance of PipeBuilder
     * @throws IllegalAccessException in case it's called before a pipe is configured
     */
    PipeBuilder name(String name) throws IllegalAccessException;

    /**
     * set a path configuration to the current pipe in the context
     * @param value path value
     * @return updated instance of PipeBuilder
     * @throws IllegalAccessException in case it's called before a pipe is configured
     */
    PipeBuilder path(String value) throws IllegalAccessException;

    /**
     * Building up a set of configurations for the current pipe
     * @param properties configuration key value pairs (must be an even number of arguments)
     * @return updated instance of PipeBuilder
     * @throws IllegalAccessException in case it's called in a bad time
     */
    PipeBuilder conf(Object... properties) throws IllegalAccessException;


    /**
     * add outputs passed key
     * @param keys properties of the outputs resource to output
     * @return current instance of PipeBuilder
     */
    PipeBuilder outputs(String ... keys);

    /**
     * builds a configured pipe. The configuration will be placed in a balanced tree under <code>/var/pipes</code>
     * @return Created (not executed) Pipe instance.
     * @throws PersistenceException error occuring when saving the pipe configuration
     */
    Pipe build() throws PersistenceException;

    /**
     * builds a configured pipe. The configuration will be placed under <code>path</code>
     * @param path path under which the generated configuration should be stored
     * @return Created (not executed) Pipe instance
     * @throws PersistenceException error occuring when saving the pipe configuration
     */
    Pipe build(String path) throws PersistenceException;

    /**
     * builds and run configured pipe
     * @return set of resource path, output of the pipe execution
     * @throws Exception exceptions thrown by the build or the pipe execution itself
     */
    ExecutionResult run() throws Exception;

    /**
     * allow execution of a pipe, with more parameter
     * @param bindings additional bindings
     * @return set of resource path, output of the pipe execution
     * @throws Exception in case something goes wrong with pipe execution
     */
    ExecutionResult run(Map bindings) throws Exception;

    /**
     * allow execution of a pipe, with more parameter
     * @param bindings additional bindings, should be key/value format
     * @return set of resource path, output of the pipe execution
     * @throws Exception in case something goes wrong with pipe execution
     */
    ExecutionResult runWith(Object... bindings) throws Exception;

    /**
     * run a pipe asynchronously
     * @param bindings additional bindings for the execution (can be null)
     * @return registered job for the pipe execution
     * @throws PersistenceException in case something goes wrong in the job creation
     */
    Job runAsync(Map bindings) throws PersistenceException;

    /**
     * run referenced pipes in parallel
     * @param numThreads number of threads to use for running the contained pipes
     * @param bindings additional bindings for the execution (can be null)
     * @return set of resource path, merged output of pipes execution (order is arbitrary)
     */
    ExecutionResult runParallel(int numThreads, Map bindings) throws Exception;
}
