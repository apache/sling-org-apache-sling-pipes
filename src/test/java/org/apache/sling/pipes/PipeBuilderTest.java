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

import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.internal.JsonUtil;
import org.junit.Test;

import javax.json.JsonObject;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PipeBuilderTest extends AbstractPipeTest {
    @Test
    public void simpleBuild() throws Exception {
        Pipe rmPipe = plumber.newPipe(context.resourceResolver())
                        .echo(PATH_APPLE)
                .rm().build();
        assertNotNull(" a basePipe should be built", rmPipe);
        //we rebuild basePipe out of created basePipe path, execute it, and test correct output (= correct basePipe built)
        testOneResource(rmPipe.getResource().getPath(), PATH_FRUITS);
    }

    @Test
    public void run() throws Exception {
        String lemonPath = "/content/fruits/lemon";
        PipeBuilder lemonBuilder = plumber.newPipe(context.resourceResolver());
        ExecutionResult result = lemonBuilder.mkdir(lemonPath).run();
        assertTrue("returned set should contain lemon path", result.getCurrentPathSet().contains(lemonPath));
        assertNotNull("there should be a lemon created", context.resourceResolver().getResource(lemonPath));
    }

    @Test
    public void dryRun() throws Exception {
        String lemonPath = "/content/fruits/lemon";
        PipeBuilder lemonBuilder = plumber.newPipe(context.resourceResolver());
        ExecutionResult result = lemonBuilder.mkdir(lemonPath).runWith("dryRun", true);
        assertFalse("returned set should not contain lemon path with dryRun=true(boolean)", result.getCurrentPathSet().contains(lemonPath));
        ExecutionResult textResult = lemonBuilder.mkdir(lemonPath).runWith("dryRun", "true");
        assertFalse("returned set should not contain lemon path with dryRun=true(text)", textResult.getCurrentPathSet().contains(lemonPath));
    }

    @Test
    public void confBuild() throws Exception {
        PipeBuilder writeBuilder = plumber.newPipe(context.resourceResolver());
        writeBuilder.echo(PATH_APPLE).write("tested", true, "working", true).run();
        ValueMap properties = context.resourceResolver().getResource(PATH_APPLE).adaptTo(ValueMap.class);
        assertTrue("properties should have been written", properties.get("tested", false) && properties.get("working", false));
    }

    @Test
    public void confContainerProperties() throws Exception {
        PipeBuilder containerBuilder = plumber.newPipe(context.resourceResolver());
        containerBuilder.with("test",true);
        String specialPath = "/content/testedContainer";
        containerBuilder.echo(PATH_APPLE).write("tested", true, "working", true).build(specialPath);
        ValueMap properties = context.resourceResolver().getResource(specialPath).adaptTo(ValueMap.class);
        assertTrue("property should have been written", properties.get("test", false));
    }

    @Test
    public void bindings() throws Exception {
        ExecutionResult result = execute("echo /content/fruits | children nt:unstructured " +
                "| grep slingPipesFilter_test=two.worm | children nt:unstructured#isnota " +
                "| children nt:unstructured @ name thing | write jcr:path=path.thing");
        assertEquals("There should be 3 resources", 3, result.size());
        String pea = "/content/fruits/apple/isnota/pea";
        String carrot = "/content/fruits/apple/isnota/carrot";
        Collection<String> paths = result.getCurrentPathSet();
        assertTrue("the paths should contain " + pea, paths.contains(pea));
        assertTrue("the paths should contain " + carrot, paths.contains(carrot));
        for (String path : paths){
            String writtenPath = context.resourceResolver().getResource(path).adaptTo(ValueMap.class).get("jcr:path", String.class);
            assertEquals("written path should be the same as actual path", path, writtenPath);
        }
    }
    @Test
    public void whitespaces() throws InvocationTargetException, IllegalAccessException {
        execute("echo /content/fruits | write jcr:title=\"white space\"");
        assertEquals("property should have been written correctly",
                "white space", context.resourceResolver().getResource(PATH_FRUITS).getValueMap().get("jcr:title"));
    }

    @Test
    public void additionalBindings() throws Exception {
        Map bindings = new HashMap<>();
        bindings.put("testedPath", PATH_FRUITS);
        ExecutionResult result = plumber.newPipe(context.resourceResolver()).echo("${testedPath}").run(bindings);
        Collection<String> paths = result.getCurrentPathSet();
        assertTrue("paths should contain implemented testedPath after run(bindings) is executed", paths.contains(PATH_FRUITS));
        paths = plumber.newPipe(context.resourceResolver())
                .echo("${testedPath}").runWith("testedPath", PATH_FRUITS)
                .getCurrentPathSet();
        assertTrue("paths should contain implemented testedPath after runWith is executed", paths.contains(PATH_FRUITS));
    }

    @Test
    public void testOutputs() throws Exception {
        ExecutionResult result = plumber.newPipe(context.resourceResolver())
                                        .echo(PATH_APPLE + "/isnota")
                                        .children("nt:unstructured")
                                            .name("vegetable")
                                            .outputs("name","vegetable['jcr:title']")
                                        .run();
        JsonObject object = JsonUtil.parseObject(result.toString());
        Collection<String> names = object.getJsonArray("items").getValuesAs(JsonObject.class)
                .stream()
                    .map(o -> o.getString("name"))
                    .collect(Collectors.toList());
        assertArrayEquals("all transformed items should be here", new String[] {"Pea", "Plum", "Carrot"}, names.toArray());
    }
}