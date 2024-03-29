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
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.Pipe;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * testing different kind of filters
 */
public class FilterPipeTest extends AbstractPipeTest {
    public static final String NN_PROPERTIES = "properties";
    public static final String NN_NOCHILDREN = "noChildrenPasses";
    public static final String NN_NOCHILDREN_FAILS = "noChildrenVoid";
    public static final String NN_TEST = "testPasses";
    public static final String NN_TEST_FAILS = "testFails";

    public void setup() throws PersistenceException {
        super.setup();
        context.load().json("/filter.json", PATH_PIPE);
    }

    @Test
    public void testFilterProperties(){
        Iterator<Resource> resourceIterator = getOutput(PATH_PIPE + "/" + NN_PROPERTIES);
        assertTrue("output has one resource...", resourceIterator.hasNext());
        ValueMap properties = resourceIterator.next().adaptTo(ValueMap.class);
        assertFalse("...and only One", resourceIterator.hasNext());
        assertEquals("output resource's color is green", "green", properties.get("color", String.class));
        assertTrue("output resource's name is not void", StringUtils.isNotBlank(properties.get("name", String.class)));
    }

    @Test
    public void testNoChildrenPasses(){
        Iterator<Resource> resourceIterator = getOutput(PATH_PIPE + "/" + NN_NOCHILDREN);
        assertTrue("output has one resource...", resourceIterator.hasNext());
        resourceIterator.next().adaptTo(ValueMap.class);
        assertFalse("...and only One", resourceIterator.hasNext());
    }

    @Test
    public void testNoChildrenFails(){
        assertFalse("output has no resource...", getOutput(PATH_PIPE + "/" + NN_NOCHILDREN_FAILS).hasNext());
    }

    @Test
    public void testTestPasses() {
        assertTrue("output has one resource...", getOutput(PATH_PIPE + "/" + NN_TEST).hasNext());
    }

    @Test
    public void testTestFails() {
        assertFalse("output has no resource...", getOutput(PATH_PIPE + "/" + NN_TEST_FAILS).hasNext());
    }

    @Test
    public void testTestPassesWithNot() throws PersistenceException {
        Resource resource = context.resourceResolver().getResource(PATH_PIPE + "/" + NN_TEST_FAILS);
        //we modify the pipe with the value NOT
        resource.adaptTo(ModifiableValueMap.class).put(FilterPipe.PN_NOT, true);
        context.resourceResolver().commit();

        Pipe pipe = plumber.getPipe(resource);
        Iterator<Resource> resourceIterator = pipe.getOutput();
        assertTrue("output has one resource...", resourceIterator.hasNext());
    }

    @Test
    public void testTestFailsWithNot() throws PersistenceException {
        Resource resource = context.resourceResolver().getResource(PATH_PIPE + "/" + NN_TEST);
        //we modify the pipe with the value NOT
        resource.adaptTo(ModifiableValueMap.class).put(FilterPipe.PN_NOT, true);
        context.resourceResolver().commit();
        Pipe pipe = plumber.getPipe(resource);
        Iterator<Resource> resourceIterator = pipe.getOutput();
        assertFalse("output has no resource...", resourceIterator.hasNext());
    }

    @Test
    public void testMultiplePattern() throws InvocationTargetException, IllegalAccessException {
        ExecutionResult result = execute("echo /content/fruits/apple/isnota | children nt:unstructured | grep sling:resourceType='.+' @ with slingPipesFilter_not=true");
        assertTrue("there should be some results", result.size() > 0);
    }

    @Test
    public void testBadTestFails() throws InvocationTargetException, IllegalAccessException {
        ExecutionResult result = execute("echo /content/fruits | grep slingPipesFilter_test=${'some string'}");
        assertEquals(0, result.size());
    }

    @Test
    public void testPropertyRegexp() throws Exception {
        Collection<String> outputs = plumber.newPipe(context.resourceResolver())
            .echo(PATH_APPLE)
            .grep("jcr:description",".*https://en.wikipedia.org.*").run().getCurrentPathSet();
        assertEquals("there should be an item", 1, outputs.size());
        outputs = plumber.newPipe(context.resourceResolver())
                .echo(PATH_APPLE)
                .grep("jcr:description",".*https://en.wikipedia.org.*").run().getCurrentPathSet();
        assertEquals("there should be an item", 1, outputs.size());
    }

    @Test
    public void testChildrenCount() throws Exception {
        Collection<String> outputs = plumber.newPipe(context.resourceResolver())
            .echo(PATH_APPLE)
            .grep("slingPipesFilter_injectChildrenCount","${true}","slingPipesFilter_test","${childrenCount==1}")
            .run().getCurrentPathSet();
        assertEquals("there should be one output", 1, outputs.size());
    }
}
