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
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.io.StringReader;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BasePipeTest extends AbstractPipeTest {

    protected BasePipe resetDryRun(Object value) throws PersistenceException {
        BasePipe basePipe = (BasePipe)plumber.newPipe(context.resourceResolver()).echo("blah").build();
        basePipe.bindings.addBinding(BasePipe.DRYRUN_KEY, value);
        return basePipe;
    }

    @Test
    public void dryRunTest() throws Exception {
        assertFalse("Is dry run should be false with flag set to text false", resetDryRun("false").isDryRun());
        assertFalse("Is dry run should be false with flag set to boolean false", resetDryRun(false).isDryRun());
        assertFalse("Is dry run should be false with no dry run flag", resetDryRun(null).isDryRun());
        assertTrue("Is dry run should be true with flag set to boolean true", resetDryRun(true).isDryRun());
        assertTrue("Is dry run should be true with flag set to text true", resetDryRun("true").isDryRun());
        assertTrue("Is dry run should be true with flag set to something that is not false or 'false'", resetDryRun("other").isDryRun());
    }

    @Test
    public void simpleErrorTest() throws Exception {
        ExecutionResult result = plumber.newPipe(context.resourceResolver()).echo("${whatever is wrong}").run();
        JsonObject response = Json.createReader(new StringReader(result.toString())).readObject();
        JsonArray array = response.getJsonArray(OutputWriter.KEY_ERRORS);
        assertEquals("there should be one error", 1, array.size());
    }

    @Test
    public void testRelativeInput() throws Exception {
        ExecutionResult results = execute("echo /content | echo fruits");
        assertEquals("it should be fruits root", PATH_FRUITS, results.currentPathSet.iterator().next());
    }

    @Test
    public void testHooks() throws  Exception {
        ResourceResolver r = context.resourceResolver();
        String beforeFlag = "before";
        String afterFlag = "after";
        Pipe before = plumber.newPipe(r).echo(PATH_FRUITS).write(beforeFlag, true).build();
        Pipe after = plumber.newPipe(r).echo(PATH_FRUITS).write(afterFlag, true).build();
        plumber.newPipe(r)
                .with(  BasePipe.PN_BEFOREHOOK, before.getResource().getPath(),
                        BasePipe.PN_AFTERHOOK, after.getResource().getPath())
                .echo(PATH_APPLE)
                .run();
        ValueMap test = r.getResource(PATH_FRUITS).getValueMap();
        assertTrue("before hook should have been called", test.get(beforeFlag, false));
        assertTrue("after hook should have been called", test.get(afterFlag, false));
    }

    @Test
    public void testSimpleBindingProviders() throws PersistenceException, IllegalAccessException {
        ResourceResolver r = context.resourceResolver();
        BasePipe pipe = (BasePipe)plumber.newPipe(r).echo(PATH_FRUITS).children("nt:unstructured[color=${test.color}]").build();
        Pipe apple = plumber.newPipe(r).name("test").echo(PATH_APPLE).build();
        assertEquals("name should be test", "test", apple.getName());
        pipe.bindingProviders = new ArrayList<>();
        pipe.bindingProviders.add(new BindingProvider(apple));
        assertTrue("there should be outputs", pipe.getOutput().hasNext());
    }
}