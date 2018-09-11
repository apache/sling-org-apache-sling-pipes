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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.util.Iterator;

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * testing executor with dummy child pipes
 */
public class ManifoldPipeTest extends AbstractPipeTest {

    public static final String NN_DEFAULT = "defaultExecutor";
    public static final String NN_STRAINED = "strainedExecutor";



    @Before
    public void setup() throws PersistenceException {
        super.setup();
        context = new SlingContext(ResourceResolverType.JCR_OAK);
        context.load().json("/SLING-INF/jcr_root/content/fruits.json", PATH_FRUITS);
        context.load().json("/threaded.json", PATH_PIPE);
    }

    @Test
    public void testWithDefaults() throws Exception {
        Iterator<Resource> output = getOutput(PATH_PIPE + "/" + NN_DEFAULT);
        boolean hasNext = output.hasNext();
        assertTrue("There should be children", hasNext);
    }

    @Test
    public void testStrained() throws Exception {
        Iterator<Resource> tenPipes = getOutput(PATH_PIPE + "/" + NN_STRAINED);
        int numResults = 0;
        while (tenPipes.hasNext()) {
             assumeNotNull("The output should not have null", tenPipes.next());
             numResults++;
        }
        assertEquals("All the sub-pipes output should be present exactly once in Executor output", 10*6, numResults);
    }

    @Test
    public void testStrainedFromReference() throws Exception {
        ExecutionResult result = plumber.newPipe(context.resourceResolver()).ref(PATH_PIPE + "/" + NN_STRAINED).run();
        assertEquals("All the sub-pipes output should be present exactly once in Executor output", 10*6, result.size());
    }

    @Test
    public void testBuilder() throws Exception {
        ExecutionResult result = plumber.newPipe(context.resourceResolver())
                .echo(PATH_APPLE)
                .echo(PATH_BANANA)
                .runParallel(2, null);
        assertEquals("There should be 2 outputs", 2, result.size());
        assertTrue("Should contain both fruits", result.toString().contains(PATH_APPLE) && result.toString().contains(PATH_BANANA));
    }
}