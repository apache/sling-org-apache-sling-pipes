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
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.internal.NopWriter;
import org.apache.sling.pipes.internal.PlumberImpl;
import org.apache.sling.testing.mock.caconfig.MockContextAwareConfig;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * testing binding's expressions instanciations
 */
public class PipeBindingsTest extends AbstractPipeTest {

    private static final String MOREBINDINGS =PATH_PIPE + "/moreBindings";

    @Before
    public void setup() throws PersistenceException {
        super.setup();
        context.load().json("/container.json", PATH_PIPE);
    }

    private PipeBindings getDummyTreeBinding() throws Exception{
        Resource resource = context.resourceResolver().getResource(PATH_PIPE + "/" + ContainerPipeTest.NN_DUMMYTREE);
        PipeBindings bindings = new PipeBindings(resource, true);
        bindings.plumber = plumber;
        return bindings;
    }

    @Test
    public void testEvaluateSimpleString() throws Exception {
        PipeBindings bindings = getDummyTreeBinding();
        String simple = "simple string";
        String evaluated = (String)bindings.evaluate(simple);
        assertEquals("evaluated should be the same than input", evaluated, simple);
    }

    @Test
    public void computeEcma5Expression() throws Exception {
        PipeBindings bindings = getDummyTreeBinding();
        Map<String,String> expressions = new HashMap<>();
        expressions.put("blah ${blah} blah", "'blah ' + blah + ' blah'");
        expressions.put("${blah}", "blah");
        expressions.put("${blah} blah", "blah + ' blah'");
        expressions.put("blah ${blah}", "'blah ' + blah");
        expressions.put("${blah}${blah}", "blah + '' + blah");
        expressions.put("+[${blah}]", "'+[' + blah + ']'");
        expressions.put("${(new Regexp('.{3}').test(path)}","(new Regexp('.{3}').test(path)");
        expressions.put("${(new Regexp('.{3,5}').test(path)}","(new Regexp('.{3,5}').test(path)");
        expressions.put("${some {{other templating}} can exist}", "some {{other templating}} can exist");
        for (Map.Entry<String,String> test : expressions.entrySet()){
            assertEquals(test.getKey() + " should be transformed in " + test.getValue(), test.getValue(), bindings.computeTemplateExpression(test.getKey()));
        }
    }

    @Test
    public void testInstantiateExpression() throws Exception {
        PipeBindings bindings = getDummyTreeBinding();
        Map<String, String> testMap = new HashMap<>();
        testMap.put("a", "apricots");
        testMap.put("b", "bananas");
        bindings.getBindings().put("test", testMap);
        String newExpression = bindings.instantiateExpression("${test.a} and ${test.b}");
        assertEquals("expression should be correctly instantiated", "apricots and bananas", newExpression);
    }

    @Test
    public void testEvaluateNull() throws Exception {
        PipeBindings bindings = getDummyTreeBinding();
        assertNull("${null} object should be instantiated as null", bindings.instantiateObject("${null}"));
        assertNull("${null} expression should be instantiated as null", bindings.instantiateExpression("${null}"));
    }

    @Test
    public void testInstantiateObject() throws Exception {
        PipeBindings bindings = getDummyTreeBinding();
        Map<String, String> testMap = new HashMap<>();
        testMap.put("a", "apricots");
        testMap.put("b", "bananas");
        bindings.getBindings().put("test", testMap);
        String newExpression = (String)bindings.instantiateObject("${test.a} and ${test.b}");
        assertEquals("expression should be correctly instantiated", "apricots and bananas", newExpression);
        /*Calendar cal = (Calendar)bindings.instantiateObject("${new Date('Sat, 12 May 2012 13:30:00 GMT')}");
        assertNotNull("calendar should be instantiated", cal);
        assertEquals("year should be correct", 2012, cal.get(Calendar.YEAR));
        assertEquals("month should be correct", 4, cal.get(Calendar.MONTH));
        assertEquals("date should be correct", 12, cal.get(Calendar.DAY_OF_MONTH));*/
    }

    @Test
    public void testAdditionalBindings() throws Exception {
        Resource resource = context.resourceResolver().getResource(MOREBINDINGS);
        BasePipe pipe = (BasePipe)plumber.getPipe(resource);
        String expression =  pipe.bindings.instantiateExpression("${three}");
        assertEquals("computed expression should be taking additional bindings 'three' in account", "3", expression);
    }

    @Test
    public void testAdditionalScript() throws Exception {
        context.load().binaryFile("/testSum.js", "/content/test/testSum.js");
        Resource resource = context.resourceResolver().getResource(MOREBINDINGS);
        BasePipe pipe = (BasePipe)plumber.getPipe(resource);
        Number expression = (Number)pipe.bindings.instantiateObject("${testSumFunction(1,2)}");
        assertEquals("computed expression have testSum script's functionavailable", 3, expression.intValue());
    }

    @Test
    public void testDisabledAdditionalScript() throws Exception {
        context.load().binaryFile("/testSum.js", "/content/test/testSum.js");
        Resource resource = context.resourceResolver().getResource(MOREBINDINGS);
        context.registerInjectActivateService(plumber, "authorizedUsers", new String[]{},
                "bufferSize", PlumberImpl.DEFAULT_BUFFER_SIZE,
                "executionPermissionResource", PATH_FRUITS,
                "mark.pipe.path",true,
                "referencesPaths", new String [] { "/conf/global/sling/pipes", "/apps/scripts" },
                "allow.additional.scripts", false);
        assertThrows(RuntimeException.class, () -> {
            BasePipe pipe = (BasePipe)plumber.getPipe(resource);
            plumber.execute(context.resourceResolver(), pipe, null, new NopWriter(), true);
        });
    }

    @Test
    public void testNameBinding() throws Exception {
        Pipe pipe = getPipe(PATH_PIPE + "/" + ContainerPipeTest.NN_ONEPIPE);
        Iterator<Resource> output = pipe.getOutput();
        output.next();
        PipeBindings bindings = pipe.getBindings();
        assertEquals("first name binding should be apple", "apple", bindings.instantiateExpression("${name.dummyParent}"));
        output.next();
        assertEquals("second name binding should be banana", "banana", bindings.instantiateExpression("${name.dummyParent}"));
    }

    @Test
    public void testCaConfigBinding() throws InvocationTargetException, IllegalAccessException {
        context.build().resource("/conf/foo/sling:configs/" + TestConfiguration.class.getName(), "fruit", "apple");
        MockContextAwareConfig.registerAnnotationClasses(context, TestConfiguration.class);
        ExecutionResult result = execute("echo /content/fruits | echo ${caconfig.one['org.apache.sling.pipes.TestConfiguration'].fruit}");
        assertTrue(result.size() > 0);
        assertEquals("/content/fruits/apple", result.currentPathSet.iterator().next());
    }

    @Test
    public void testContextualError() throws InvocationTargetException, IllegalAccessException {
        ExecutionResult first = execute("json {'one':{'foo':'bar'},'two':{'another':'one'},'three':{'foo':'longer'}} | " +
                "mkdir /content/context/${one.key} | " +
                "write ${one.value.foo?'foo':'blah'}=one.value.foo");
        //the following should fail for /content/context/two
        ExecutionResult result = execute("echo /content/context " +
                "| children sling:Folder @ name child " +
                "| write foo=${child.foo?child.foo:nonexistingFunction(nonexistingVariable)}");
        assertEquals(2, result.size());
    }

}