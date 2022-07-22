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
package org.apache.sling.pipes.internal.inputstream;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.ExecutionResult;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aMultipart;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * testing json pipe with anonymous yahoo meteo API
 */
public class JsonPipeTest extends AbstractPipeTest {
    public static final String CONTENT_JSON = "/content/json";
    public static final String CONF = CONTENT_JSON + "/conf/weather";
    public static final String ARRAY = CONTENT_JSON + "/conf/array";

    @Before
    public void setup() throws PersistenceException {
        super.setup();
        context.load().json("/json.json", "/content/json");
    }

    @Rule
    public WireMockRule http = new WireMockRule(PORT);


    @Test
    public void testPipedJson() throws Exception{
        Iterator<Resource> outputs = getOutput(CONF);
        outputs.next();
        Resource result = outputs.next();
        context.resourceResolver().commit();
        ValueMap properties = result.adaptTo(ValueMap.class);
        assertTrue("There should be a Paris property", properties.containsKey("Paris"));
        assertTrue("There should be a Bucharest property", properties.containsKey("Bucharest"));
    }

    protected void testArray(Iterator<Resource> outputs){
        Resource first = outputs.next();
        Resource second = outputs.next();
        Resource third = outputs.next();
        assertFalse("there should be only three elements", outputs.hasNext());
        assertEquals("first resource should be one", "/content/json/array/one", first.getPath());
        assertEquals("second resource should be two", "/content/json/array/two", second.getPath());
        assertEquals("third resource should be three", "/content/json/array/three", third.getPath());
    }

    @Test
    public void testPipedArray() throws Exception {
        testArray(getOutput(ARRAY));
    }
    protected void testJsonPath(String json, String valuePath) throws Exception {
        assertEquals("there should be 2 results for valuePath " + valuePath, 2, plumber.newPipe(context.resourceResolver())
                .echo("/content/fruits")
                .json(json).with("valuePath", valuePath).name("json")
                .echo("/content/json/array/${json.test}")
                .run().size());
    }

    @Test
    public void testSimpleJsonPath() throws Exception {
        testJsonPath("{'size':2, 'items':[{'test':'one'}, {'test':'two'}]}", "$.items");
        testJsonPath("[['foo','bar'],[{'test':'one'}, {'test':'two'}]]", "$[1]");
    }
    @Test
    public void testNestedJsonPath()  throws Exception {
        testJsonPath("{'arrays':[['foo','bar'],[{'test':'one'}, {'test':'two'}]]}", "$.arrays[1]");
        testJsonPath("{'objects':{'items':[{'test':'one'}, {'test':'two'}]}}", "$.objects.items");
    }

    @Test
    public void testDynamicJsonPath() throws Exception {
        testJsonPath("{'foo':[{'test':'one'}, {'test':'two'}]}", "$.${(true?'foo':'bar')}");
    }

    @Test
    public void testSimpleRemoteJson() throws InvocationTargetException, IllegalAccessException {
        http.givenThat(get(urlEqualTo("/get/foo.json"))
                .willReturn(aResponse().withStatus(200).withBody("{\"args\":{\"foo1\":\"bar\"}}")));
        ExecutionResult results = execute("echo /content " +
                "| json "  + baseUrl + "/get/foo.json @ name data @ with raw=true " +
                "| mkdir ${data.args.foo1}");
        assertEquals(1, results.size());
        assertEquals("/content/bar", results.getCurrentPathSet().iterator().next());
    }

    @Test
    public void testPostJsonFormUrlEncoded() throws InvocationTargetException, IllegalAccessException {
        http.givenThat(post(urlEqualTo("/post/foo.json"))
                .withHeader("x-header", equalTo("bar"))
                .withRequestBody(matching("(.*&|^)foo=1($|&.*)"))
                .willReturn(aResponse().withStatus(201).withBody("{\"created\":\"true\"}")));
        ExecutionResult results = execute("echo /content " +
                "| json "  + baseUrl + "/post/foo.json?foo=1 @ name data @ with raw=true POST=true header_x-header=bar" +
                "| write created=${data.created}");
        assertEquals(1, results.size());
        assertEquals("true", context.resourceResolver().getResource("/content").getValueMap().get("created"));
    }

    @Test
    public void testPostJsonRawBody() throws InvocationTargetException, IllegalAccessException {
        http.givenThat(post(urlEqualTo("/post/foo.json"))
                .withHeader("x-header", equalTo("bar"))
                .withRequestBody(matching("(.*&|^)\\{foo:1\\}($|&.*)"))
                .willReturn(aResponse().withStatus(201).withBody("{\"created\":\"true\"}")));
        ExecutionResult results = execute("echo /content " +
                "| json "  + baseUrl + "/post/foo.json @ name data @ with raw=true POST={foo:1} header_x-header=bar" +
                "| write created=${data.created}");
        assertEquals(1, results.size());
        assertEquals("true", context.resourceResolver().getResource("/content").getValueMap().get("created"));
    }

    @Test
    public void testLoopOverObject() throws InvocationTargetException, IllegalAccessException {
        ExecutionResult results = execute("echo /content " +
                "| json {'k1':'v1','k2':'v2'} @ name j" +
                "| mkdir ${j.key}/${j.value}");
        assertEquals(2, results.size());
        List<String> array = IteratorUtils.toList(results.getCurrentPathSet().iterator());
        assertArrayEquals(new String[] {"/content/k2/v2", "/content/k1/v1"}, array.toArray());
    }

    @Test
    public void testRaw() throws InvocationTargetException, IllegalAccessException {
        ExecutionResult results = execute("echo /content " +
                "| json {'k1':'v1','k2':'v2'} @ name j @ with raw=true" +
                "| write test=${j.k2}");
        assertEquals(1, results.size());
        assertEquals("v2", context.resourceResolver().getResource("/content").getValueMap().get("test", String.class));
    }

    @Test
    public void testIndex() throws InvocationTargetException, IllegalAccessException {
        ExecutionResult results = execute("echo /content " +
                "| json [\"blah\",\"blah\"] @ name dumbArray " +
                "| json {\"foo\":\"blah\",\"bar\":\"blah\"} @ name dumbObject" +
                "| mkdir /content/o_${dumbObject_index}/a_${dumbArray_index}");
        assertEquals(4, results.size());
        assertTrue(results.toString().contains("[\"/content/o_0/a_0\",\"/content/o_1/a_0\",\"/content/o_0/a_1\",\"/content/o_1/a_1\"]"));
    }

    @Test
    @Ignore
    public void testAuthentifiedRemoteJson() throws InvocationTargetException, IllegalAccessException {
        http.givenThat(get(urlEqualTo("/get/profile.json")).withBasicAuth("jdoe","jdoe")
                .willReturn(aResponse().withStatus(200).withBody("{\"firstName\":\"John\"}")));
        ExecutionResult results = plumber.newPipe(context.resourceResolver())
                .json(baseUrl + "/get/profile.json").name("profile")
                .mkdir("/home/${profile.firstName}").runWith("basicAuth", "jdoe:jdoe");
        assertEquals(1, results.size());
        assertEquals("/home/John", results.getCurrentPathSet().iterator().next());
    }


}