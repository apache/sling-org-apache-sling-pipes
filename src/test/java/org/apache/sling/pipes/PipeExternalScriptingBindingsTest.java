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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.junit.Before;
import org.junit.Test;

/**
 * testing binding's expressions instanciations
 */
public class PipeExternalScriptingBindingsTest extends AbstractPipeTest {

    private static final String MOREBINDINGS =PATH_PIPE + "/moreBindings";

    private static final String JS_ENGINE = "nashorn";
    private static final String GROOVY_ENGINE = "groovy";

    @Before
    public void setup() throws PersistenceException {
        super.setup();
        context.load().json("/container.json", PATH_PIPE);
    }

    private PipeBindings getDummyTreeBinding(String scriptEngine) throws Exception{
        Resource resource = context.resourceResolver().getResource(PATH_PIPE + "/" + ContainerPipeTest.NN_DUMMYTREE);
        PipeBindings bindings = new PipeBindings(resource, true);
        bindings.initializeScriptEngine(scriptEngine);
        return bindings;
    }

    @Test
    public void testInstantiateExpression() throws Exception {
        PipeBindings bindings = getDummyTreeBinding(JS_ENGINE);
        Map<String, String> testMap = new HashMap<>();
        testMap.put("a", "apricots");
        testMap.put("b", "bananas");
        bindings.getBindings().put("test", testMap);
        String newExpression = bindings.instantiateExpression("${test.a} and ${test.b}");
        assertEquals("expression should be correctly instantiated", "apricots and bananas", newExpression);
    }

    @Test
    public void testEvaluateNull() throws Exception {
        PipeBindings bindings = getDummyTreeBinding(JS_ENGINE);
        assertNull("${null} object should be instantiated as null", bindings.instantiateObject("${null}"));
        assertNull("${null} expression should be instantiated as null", bindings.instantiateExpression("${null}"));
    }

    @Test
    public void testInstantiateObject() throws Exception {
        PipeBindings bindings = getDummyTreeBinding(JS_ENGINE);
        Map<String, String> testMap = new HashMap<>();
        testMap.put("a", "apricots");
        testMap.put("b", "bananas");
        bindings.getBindings().put("test", testMap);
        String newExpression = (String)bindings.instantiateObject("${test.a} and ${test.b}");
        assertEquals("expression should be correctly instantiated", "apricots and bananas", newExpression);
    }

    @Test
    public void testJSAdditionalScript() throws Exception {
        context.load().binaryFile("/testSum.js", "/content/test/testSum.js");
        Resource resource = context.resourceResolver().getResource(MOREBINDINGS);
        BasePipe pipe = (BasePipe)plumber.getPipe(resource);
        Number expression = (Number)pipe.bindings.instantiateObject("${testSumFunction(1,2)}");
        assertEquals("computed expression have testSum script's functionavailable", 3, expression.intValue());
    }

    @Test
    public void testGroovyPipe() throws Exception {
        ExecutionResult executionResult = plumber.newPipe(context.resourceResolver())
            .echo("/content")
            .mkdir("hello/${\"$who\"}")
            .runWith("engine","groovy","who","world");
        assertNotNull(executionResult);
        assertEquals("/content/hello/world", executionResult.currentPathSet.iterator().next());
    }
}