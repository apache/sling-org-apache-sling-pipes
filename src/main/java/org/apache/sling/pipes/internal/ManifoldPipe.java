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

import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.NonExistingResource;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.OutputWriter;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.pipes.SuperPipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This pipe executes the pipes it has in its configuration, in sequence or parallel;
 * the output of the children pipes is merged;
 * if execution is parallel, merge ordering is random;
 * duplicate resources are kept in the output
 * ManifoldPipe uses a thread pool to run its subpipes, but is NOT itself thread-safe
 */
public class ManifoldPipe extends SuperPipe {
    private static final Logger log = LoggerFactory.getLogger(ManifoldPipe.class);

    public static final String RESOURCE_TYPE = "slingPipes/manifold";
    public static final String PN_QUEUE_SIZE = "queueSize";
    public static final String PN_NUM_THREADS = "numThreads";
    public static final String PN_EXECUTION_TIMEOUT = "executionTimeout";
    public static final int QUEUE_SIZE_DEFAULT = 10000;
    public static final int NUM_THREADS_DEFAULT = 5;
    public static final int EXECUTION_TIMEOUT_DEFAULT = 24*60*60;
    // marker to be inserted in the queue after all thread pipes are done pushing output
    private static final Resource END_OF_STREAM = new NonExistingResource(null, "");

    private int numThreads;
    private int executionTimeout;
    private ArrayBlockingQueue<Resource> outputQueue;

    /**
     * Constructor
     * @param plumber plumber
     * @param resource container's configuration resource
     * @param upperBindings pipe bindings
     */
    public ManifoldPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
        int queueSize = properties.get(PN_QUEUE_SIZE, QUEUE_SIZE_DEFAULT);
        numThreads = properties.get(PN_NUM_THREADS, NUM_THREADS_DEFAULT);
        executionTimeout = properties.get(PN_EXECUTION_TIMEOUT, EXECUTION_TIMEOUT_DEFAULT);
        outputQueue = new ArrayBlockingQueue<>(queueSize);
    }

    @Override
    public void buildChildren() {
        for (Iterator<Resource> childPipeResources = getConfiguration().listChildren(); childPipeResources.hasNext();){
            Resource pipeResource = childPipeResources.next();
            Pipe pipe = plumber.getPipe(pipeResource, bindings);
            if (pipe == null) {
                log.error("configured pipe {} is either not registered, or not computable by the plumber", pipeResource.getPath());
            } else {
                pipe.setParent(this.getParent());
                subpipes.add(pipe);
            }
        }
    }

    @Override
    protected Iterator<Resource> computeSubpipesOutput() {
        return new ConcurrentIterator(numThreads);
    }

    private class PipeThread implements Runnable {

        Pipe pipe;

        PipeThread(Pipe pipe) {
            this.pipe = pipe;
        }

        @Override
        public void run() {
            try {
                plumber.execute(pipe.getResource().getResourceResolver().clone(null), pipe, null, new ThreadOutputWriter(), true);
            } catch (Exception e) {
                log.error("Error while running pipe %s", pipe.getName(), e);
            }
        }
    }

    private class ThreadOutputWriter extends OutputWriter {
        @Override
        protected void writeItem(Resource resource) {
            try {
                outputQueue.put(resource);
            } catch (InterruptedException e) {
                log.error("Interrupted while running pipe %s", pipe.getName(), e);
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public boolean handleRequest(SlingHttpServletRequest request) {
            return false;
        }

        @Override
        protected void initResponse(SlingHttpServletResponse response) {/*no handling here*/}

        @Override
        public void starts() {/*no handling here*/}

        @Override
        public void ends() {/*no handling here*/}
    }

    private class ConcurrentIterator implements Iterator<Resource> {

        private ExecutorService executorService;
        private Resource nextItem = null;

        private class StreamTerminator implements Runnable {
            @Override
            public void run() {
                try {
                    executorService.awaitTermination(executionTimeout, TimeUnit.SECONDS);
                    outputQueue.put(END_OF_STREAM);
                } catch (InterruptedException e) {
                    log.error("Interrupted while waiting for input exhaustion", e);
                    Thread.currentThread().interrupt();
                }
            }
        }

        ConcurrentIterator(int numThreads) {
            executorService = Executors.newFixedThreadPool(numThreads);
            for (Pipe pipe: subpipes) {
                executorService.execute(new PipeThread(pipe));
            }
            executorService.shutdown();
            new Thread(new StreamTerminator()).start();
        }

        @Override
        public boolean hasNext() {
            peekNext();
            return nextItem != END_OF_STREAM;
        }

        @Override
        public Resource next() {
            peekNext();
            if (nextItem == END_OF_STREAM) {
                throw new NoSuchElementException();
            }
            Resource toReturn = nextItem;
            nextItem = null;
            return toReturn;
        }

        private void peekNext() {
            if (nextItem == null) {
                try {
                    nextItem = outputQueue.take();
                } catch (InterruptedException e) {
                    log.error("Interrupted while retrieving output", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
