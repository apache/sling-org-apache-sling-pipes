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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * holds results of the execution
 */
public class ExecutionResult {

    /**
     * *not* meant to hold the all the paths, just a set that is emptied each time
     * it's persisted.
     */
    Set<String> currentPathSet;

    OutputWriter writer;

    /**
     * Constructor
     * @param writer output writer around which to create the result
     */
    public ExecutionResult(OutputWriter writer) {
        this.writer = writer;
        currentPathSet = new HashSet<>();
    }

    /**
     * Add a resource to the results
     * @param resource resource to add
     */
    public void addResultItem(Resource resource) {
        writer.write(resource);
        currentPathSet.add(resource.getPath());
    }

    /**
     * Empty the current path set
     */
    public void emptyCurrentSet(){
        currentPathSet.clear();
    }

    /**
     * return currentPathSet
     * @return current path set
     */
    public Collection<String> getCurrentPathSet() {
        return currentPathSet;
    }

    /**
     * amount of changed items
     * @return total size of changed items
     */
    public long size(){
        return writer.size;
    }

    @Override
    public String toString() {
        return writer.toString();
    }
}
