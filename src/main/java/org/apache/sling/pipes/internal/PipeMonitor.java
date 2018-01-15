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

import org.apache.sling.event.jobs.Job;
import org.apache.sling.pipes.BasePipe;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.pipes.Plumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.util.HashMap;
import java.util.Map;

public class PipeMonitor implements PipeMonitorMBean {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PipeMonitor.class);

    String name;

    String path;

    boolean running = false;

    String status;

    long lastStarted;

    long duration;

    int executions = 0;

    int failed = 0;

    long mean;

    Plumber plumber;

    ExecutionResult lastResult;

    public void starts(){
        lastStarted = System.currentTimeMillis();
        running = true;
        status = BasePipe.STATUS_STARTED;
    }

    public void ends() {
        duration = System.currentTimeMillis() - lastStarted;
        mean = ((mean * executions) + duration) / (executions + 1);
        executions ++;
        running = false;
        status = BasePipe.STATUS_FINISHED;
    }

    public long getFailed(){
        return failed;
    }

    @Override
    public String getStatus() {
        return status;
    }

    public void failed() {
        failed ++;
        running = false;
        status = BasePipe.STATUS_FINISHED;
    }

    public PipeMonitor(Plumber currentPlumber, Pipe pipe){
        plumber = currentPlumber;
        name = pipe.getName();
        path = pipe.getResource().getPath();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public long getExecutionCount() {
        return executions;
    }

    @Override
    public long getMeanDurationMilliseconds() {
        return mean;
    }

    public void setLastResult(ExecutionResult result) {
        lastResult = result;
    }

    @Override
    public CompositeData getLastResult() {
        try {
            if (lastResult != null){
                return lastResult.asCompositeData();
            }
        } catch (OpenDataException e) {
            LOGGER.error("unable to dump last result as composite data", e);
        }
        return null;
    }

    @Override
    public String run() {
        Map bindings = new HashMap<>();
        bindings.put(BasePipe.READ_ONLY, false);
        Job job = plumber.executeAsync(path, bindings);
        return String.format("Job %s has been created", job.getId());
    }
}
