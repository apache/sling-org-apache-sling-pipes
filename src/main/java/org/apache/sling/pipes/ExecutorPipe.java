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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.sling.api.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipe executes the pipes it has in its configuration, in sequence or parallel;
 * the output of the children pipes is merged;
 * if execution is parallel, merge ordering is random;
 * duplicate resources are kept in the output
 */
public class ExecutorPipe extends BasePipe {
    private static final Logger log = LoggerFactory.getLogger(ExecutorPipe.class);

    public static final String RESOURCE_TYPE = "slingPipes/executor";
    private static final String PN_QUEUE_SIZE = "queueSize";
    private static final String PN_NUM_THREADS = "numThreads";

    private int queueSize;
    private int numThreads;
    private List<PipeThread> pipeThreads = new ArrayList<>();
    private ReadWriteLock queueCheckLock = new ReentrantReadWriteLock();
    private Condition resourceQueued = queueCheckLock.writeLock().newCondition();

    /**
     * Constructor
     * @param plumber plumber
     * @param resource container's configuration resource
     * @param upperBindings pipe bindings
     * @throws Exception bad configuration handling
     */
    public ExecutorPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) throws Exception{
        super(plumber, resource, upperBindings);
        queueSize = properties.get(PN_QUEUE_SIZE, 1000);
        numThreads = properties.get(PN_NUM_THREADS, 10);
        for (Iterator<Resource> childPipeResources = getConfiguration().listChildren(); childPipeResources.hasNext();){
            Resource pipeResource = childPipeResources.next();
            Pipe pipe = plumber.getPipe(pipeResource, bindings);
            if (pipe == null) {
                log.error("configured pipe {} is either not registered, or not computable by the plumber", pipeResource.getPath());
            } else {
                pipe.setParent(this.getParent());
                pipeThreads.add(new PipeThread(pipe));
            }
        }
    }

    @Override
    public boolean modifiesContent() {
        for (PipeThread pipeThread: pipeThreads) {
            if (pipeThread.pipe.modifiesContent()) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Iterator<Resource> computeOutput() throws Exception {
        return new ConcurrentIterator(numThreads);
    }

    private class PipeThread implements Runnable {

        Pipe pipe;
        ArrayBlockingQueue<Resource> outputQueue;
        boolean isDone = false;

        PipeThread(Pipe pipe) {
            this.pipe = pipe;
            this.outputQueue = new ArrayBlockingQueue<>(queueSize);
        }

        @Override
        public void run() {
            Iterator<Resource> outputIterator = pipe.getOutput();
            while (outputIterator.hasNext()) {
                Resource nextItem = outputIterator.next();
                if (nextItem == null) {
                    // session was closed, exit thread
                    break;
                }
                try {
                    queueCheckLock.writeLock().lock();
                    if (!outputQueue.offer(nextItem)) {
                        // not nice, but keeping the write lock while we wait for queue would block the whole Executor
                        queueCheckLock.writeLock().unlock();
                        outputQueue.put(nextItem);
                        queueCheckLock.writeLock().lock();
                    }
                    resourceQueued.signal();
                } catch (InterruptedException e) {
                    log.error("Interrupted while running pipe %s", pipe.getName(), e);
                } finally {
                    queueCheckLock.writeLock().unlock();
                }
            }
            isDone = true;
        }

        boolean hasMore() {
            return outputQueue.size() > 0 || !isDone;
        }
    }

    private class ConcurrentIterator implements Iterator<Resource> {

        private final ExecutorService executorService;

        ConcurrentIterator(int numThreads) {
            executorService = Executors.newFixedThreadPool(numThreads);
            for (PipeThread pipeThread: pipeThreads) {
                executorService.execute(pipeThread);
            }
        }

        @Override
        public boolean hasNext() {
            try {
                queueCheckLock.readLock().lock();
                for (PipeThread pipeThread : pipeThreads) {
                    if (pipeThread.hasMore()) {
                        return true;
                    }
                }
            } finally {
                queueCheckLock.readLock().unlock();
            }
            executorService.shutdown();
            return false;
        }

        @Override
        public Resource next() {
            queueCheckLock.readLock().lock();
            for (PipeThread pipeThread: pipeThreads) {
                if (pipeThread.outputQueue.size() > 0) {
                    Resource nextItem = pipeThread.outputQueue.poll();
                    queueCheckLock.readLock().unlock();
                    return nextItem;
                }
            }
            // all queues empty, check state
            if (!hasNext()) {
                queueCheckLock.readLock().unlock();
                throw new NoSuchElementException();
            }
            // can't upgrade to a write lock, and can't have a Condition on the read lock
            // so there's this liiitle crack here between unlock and lock, where shit could happen
            queueCheckLock.readLock().unlock();
            queueCheckLock.writeLock().lock();
            try {
                // expecting more data, wait for one of the pipes to push some
                resourceQueued.await(100, TimeUnit.MICROSECONDS);
                // someone farted, find the culprit
                return next();
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for pipe output", e);
                throw new IllegalStateException("Interrupted while waiting for pipe output", e);
            } finally {
                queueCheckLock.writeLock().unlock();
            }
        }
    }
}
