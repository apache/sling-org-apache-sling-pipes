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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.Pipe;
import org.junit.Before;
import org.junit.Test;

/**
 * testing references
 */
public class ShallowReferencePipeTest extends AbstractPipeTest {

    @Before
    public void setUp() throws Exception {
        context.load().json("/reference.json", PATH_PIPE);
    }

    @Test
    public void testDynamicBinding() throws Exception {
        plumber.newPipe(context.resourceResolver()).echo(PATH_FRUITS + "/apple").build(PATH_PIPE + "/applePipe");
        plumber.newPipe(context.resourceResolver()).echo(PATH_FRUITS + "/banana").build(PATH_PIPE + "/bananaPipe");

        ExecutionResult result = execute("json ['apple','banana'] @ name fruit" +
                " | shallowRef " + PATH_PIPE + "/${fruit}Pipe");
        assertEquals("there should be two outputs", 2, result.size());
        assertTrue("apple should be returned", result.getCurrentPathSet().contains(PATH_FRUITS + "/apple"));
        assertTrue("banana should be returned", result.getCurrentPathSet().contains(PATH_FRUITS + "/banana"));
    }
}