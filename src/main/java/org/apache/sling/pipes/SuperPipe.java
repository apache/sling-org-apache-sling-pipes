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

import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Pipe that outputs a configured set of pipes output, managing for them what is their outputs, and bindings
 */
public abstract class SuperPipe extends BasePipe {
    private static final Logger LOG = LoggerFactory.getLogger(SuperPipe.class);

    /**
     * Sleep time, in ms, after each resource returned
     */
    public static final String PN_SLEEP = "sleep";

    protected long sleep = 0L;

    protected List<Pipe> subpipes = new ArrayList<>();

    /**
     * Pipe Constructor
     *
     * @param plumber       plumber
     * @param resource      configuration resource
     * @param upperBindings already set bindings, can be null
     */
    public SuperPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
        sleep = properties.get(PN_SLEEP, 0L);
    }

    /**
     * build the subpipes pipes list
     */
    public abstract void buildChildren();

    /**
     * @return output of this super pipe's subpipes
     */
    protected abstract Iterator<Resource> computeSubpipesOutput();

    @Override
    protected Iterator<Resource> computeOutput() {
        if (subpipes.isEmpty()){
            buildChildren();
        }
        return computeSubpipesOutput();
    }

    /**
     * Return the first pipe in the container
     * @return first pipe of the container
     */
    protected Pipe getFirstPipe() {
        return !subpipes.isEmpty() ? subpipes.get(0) : null;
    }

    /**
     * Return the last pipe in the container
     * @return pipe in the last position of the container's pipes
     */
    protected Pipe getLastPipe() {
        return !subpipes.isEmpty() ? subpipes.get(subpipes.size() - 1) : null;
    }

    /**
     * Return the previous pipe of the given child pipe
     * @param pipe child pipe of this parent
     * @return previous pipe if any
     */
    public Pipe getPreviousPipe(Pipe pipe){
        Pipe previousPipe = null;
        if (!subpipes.isEmpty()){
            if (subpipes.get(0).equals(pipe) && parent != null){
                //in the case this pipe has a parent, previous pipe is the one of the referrer
                return parent.getPreviousPipe(this);
            }
            for (Pipe candidate : subpipes){
                if (candidate.equals(pipe)){
                    return previousPipe;
                }
                previousPipe = candidate;
            }
        }
        return null;
    }

    @Override
    public boolean modifiesContent() {
        try {
            if (subpipes.isEmpty()) {
                buildChildren();
            }
            for (Pipe pipe : subpipes){
                if (pipe.modifiesContent()){
                    return true;
                }
            }
            return false;
        } catch (Exception e){
            LOG.error("something went wrong while building this pipe, we'll consider this pipe as modifying content", e);
        }
        return true;
    }

    @Override
    public void before() {
        LOG.debug("entering {} before", getName());
        super.before();
        if (subpipes.isEmpty()){
            buildChildren();
        }
        for (Pipe pipe : subpipes){
            LOG.debug("calling {} before", getName());
            pipe.before();
        }
    }

    @Override
    public void after() {
        LOG.debug("entering {} after", getName());
        super.after();
        for (Pipe pipe : subpipes){
            LOG.debug("calling {} after", pipe.getName());
            pipe.after();
        }
    }
}
