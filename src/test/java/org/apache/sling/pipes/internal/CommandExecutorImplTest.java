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

import org.apache.commons.io.IOUtils;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.PipeBuilder;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.apache.sling.testing.mock.sling.servlet.MockSlingHttpServletRequest;
import org.apache.sling.testing.mock.sling.servlet.MockSlingHttpServletResponse;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.servlet.ServletException;

public class CommandExecutorImplTest extends AbstractPipeTest {

    CommandExecutorImpl commands;

    @Before
    public void setup() throws PersistenceException {
        super.setup();
        context = new SlingContext(ResourceResolverType.JCR_OAK);
        context.load().json("/initial-content/content/fruits.json", PATH_FRUITS);
        commands = new CommandExecutorImpl();
        commands.plumber = plumber;
    }

    @Test
    public void testParseTokens(){
        List<CommandExecutorImpl.Token> tokens = commands.parseTokens("some", "isolated", "items");
        assertEquals("there should be 1 token", 1, tokens.size());
        CommandExecutorImpl.Token token = tokens.get(0);
        assertEquals("pipe key should be 'some'","some", token.pipeKey);
        assertEquals("pipe args should be isolated, items", Arrays.asList("isolated","items"), token.args);
        String tokenString = "first arg / second firstarg secondarg @ name second / third blah";
        tokens = commands.parseTokens(tokenString.split("\\s"));
        assertEquals("there should be 3 tokens", 3, tokens.size());
        assertEquals("keys check", Arrays.asList("first","second", "third"), tokens.stream().map(t -> t.pipeKey).collect(Collectors.toList()));
        assertEquals("params check", "second", tokens.get(1).options.name);
    }

    @Test
    public void testKeyValueToArray() {
        assertArrayEquals(new String[]{"one","two","three","four"}, commands.keyValuesToArray(Arrays.asList("one=two","three=four")));
        assertArrayEquals(new String[]{"one","two","three","${four}"}, commands.keyValuesToArray(Arrays.asList("one=two","three=${four}")));
        assertArrayEquals(new String[]{"one","two","three","${four == 'blah' ? 'five' : 'six'}"},
            commands.keyValuesToArray(Arrays.asList("one=two","three=${four == 'blah' ? 'five' : 'six'}")));
        assertArrayEquals(new String[]{"jcr:content/singer","${'ringo' == one ? false : true}"}, commands.keyValuesToArray(Arrays.asList("jcr:content/singer=${'ringo' == one ? false : true}")));
    }

    @Test
    public void testSimpleExpression() throws Exception {
        PipeBuilder builder = commands.parse(context.resourceResolver(),"echo","/content/fruits");
        assertTrue("there should be a resource", builder.build().getOutput().hasNext());
    }

    @Test
    public void testSimpleChainedConf() throws Exception {
        PipeBuilder builder = commands.parse(context.resourceResolver(),"echo /content/fruits / write some=test key=value".split("\\s"));
        assertNotNull("there should be a resource", builder.run());
        ValueMap props = context.currentResource(PATH_FRUITS).getValueMap();
        assertEquals("there should some=test", "test", props.get("some"));
        assertEquals("there should key=value", "value", props.get("key"));
    }

    @Test
    public void testOptions() {
        String expected = "works";
        String optionString = "@ name works @ path works @ expr works @ with one=works two=works @ outputs one=works two=works";
        CommandExecutorImpl.Options options = commands.getOptions(optionString.split("\\s"));
        assertEquals("check name", expected, options.name);
        assertEquals("check expr", expected, options.expr);
        assertEquals("check path", expected, options.path);
        Map bindings = new HashMap();
        CommandUtil.writeToMap(bindings, options.with);
        assertEquals("check with first", expected, bindings.get("one"));
        assertEquals("check with second", expected, bindings.get("two"));
        assertNotNull("a writer should have been created", options.writer);
        Map outputs = options.writer.getCustomOutputs();
        assertEquals("check writer first", expected, outputs.get("one"));
        assertEquals("check writer second", expected, outputs.get("two"));
    }

    @Test
    public void testOptionsListsWithOneItem() {
        String expected = "works";
        String optionString = "@ with one=works @ outputs one=works";
        CommandExecutorImpl.Options options = commands.getOptions(optionString.split("\\s"));
        Map bindings = new HashMap();
        CommandUtil.writeToMap(bindings, options.with);
        assertEquals("check with first", expected, bindings.get("one"));
        assertNotNull("a writer should have been created", options.writer);
        Map outputs = options.writer.getCustomOutputs();
        assertEquals("check writer first", expected, outputs.get("one"));
    }

    @Test
    public void testChainedConfWithInternalOptions() throws Exception {
        PipeBuilder builder = commands.parse(context.resourceResolver(),
        "echo /content/fruits @ name fruits / write some=${path.fruits} key=value".split("\\s"));
        assertNotNull("there should be a resource", builder.run());
        ValueMap props = context.currentResource(PATH_FRUITS).getValueMap();
        assertEquals("there should some=/content/fruits", PATH_FRUITS, props.get("some"));
        assertEquals("there should key=value", "value", props.get("key"));
    }

    @Test
    public void adaptToDemoTest() throws Exception {
        String url = "'http://99-bottles-of-beer.net/lyrics.html'";
        String cmd = "egrep " + url + " @ name bottles @ with 'pattern=(?<number>\\d(\\d)?)' / mkdir '/var/bottles/${bottles.number}'";
        PipeBuilder builder = commands.parse(context.resourceResolver(), cmd.split("\\s"));
        ContainerPipe pipe = (ContainerPipe)builder.build();
        ValueMap regexp = pipe.getResource().getChild("conf/bottles").getValueMap();
        assertEquals("we expect expr to be the url", url, regexp.get("expr"));
    }

    @Test
    public void testExecuteWithWriter() throws Exception {
        PipeBuilder builder = plumber.newPipe(context.resourceResolver()).echo("/content/${node}").$("nt:base");
        String path = builder.build().getResource().getPath();
        ExecutionResult result = commands.execute(context.resourceResolver(), path, "@ outputs title=jcr:title desc=jcr:description @ with node=fruits");
    }

    String testServlet(Map<String,Object> params) throws ServletException, IOException {
        MockSlingHttpServletRequest request = context.request();
        MockSlingHttpServletResponse response = context.response();
        request.setParameterMap(params);
        request.setMethod("POST");
        commands.doPost(request, response);
        if (response.getStatus() != 200) {
            System.out.println(response.getOutputAsString());
        }
        assertEquals(200, response.getStatus());
        return response.getOutputAsString();
    }

    @Test
    public void testSimpleCommandServlet() throws IOException, ServletException {
        Map<String, Object> params = new HashMap<>();
        params.put(CommandExecutorImpl.REQ_PARAM_CMD, "echo /content / mkdir foo / write type=bar");
        String response = testServlet(params);
        assertEquals("{\"items\":[\"/content/foo\"],\"size\":1}\n", response);
    }

    @Test
    public void testFileCommandServlet() throws IOException, ServletException {
        Map<String, Object> params = new HashMap<>();
        params.put(CommandExecutorImpl.REQ_PARAM_FILE, IOUtils.toString(getClass().getResourceAsStream("/testcommand"
            + ".txt"), "UTF-8"));
        String response = testServlet(params);
        assertEquals("{\"items\":[\"/content/beatles/john\",\"/content/beatles/paul\","
            + "\"/content/beatles/georges\",\"/content/beatles/ringo\"],"
            + "\"size\":4}\n"
            + "{\"items\":[\"/content/beatles/ringo/jcr:content\"],\"size\":1}\n", response);
    }
}