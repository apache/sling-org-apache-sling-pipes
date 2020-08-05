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

import java.lang.reflect.InvocationTargetException;

import org.apache.sling.api.resource.ResourceResolver;

public interface CommandExecutor {

    /**
     * internal execution command handler
     * @param resolver resolver with which pipe will be executed
     * @param path pipe path to execute
     * @param options different options tokens
     * @return Execution results
     */
    ExecutionResult execute(ResourceResolver resolver, String path, String... options);

    /**
     * @param resolver resource resolver with which pipe will build the pipe
     * @param commands list of commands for building the pipe
     * @return PipeBuilder instance (that can be used to finalize the command)
     * @throws InvocationTargetException can happen in case the mapping with PB api went wrong
     * @throws IllegalAccessException can happen in case the mapping with PB api went wrong
     */
    PipeBuilder parse(ResourceResolver resolver, String... commands) throws InvocationTargetException, IllegalAccessException;

    /**
     * @return help string
     */
    String help();
}
