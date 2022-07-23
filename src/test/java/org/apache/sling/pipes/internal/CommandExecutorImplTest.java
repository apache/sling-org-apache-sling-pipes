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
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.CommandUtil;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.PipeBuilder;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.apache.sling.testing.mock.sling.servlet.MockRequestPathInfo;
import org.apache.sling.testing.mock.sling.servlet.MockSlingHttpServletRequest;
import org.apache.sling.testing.mock.sling.servlet.MockSlingHttpServletResponse;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static org.apache.sling.pipes.internal.CommandExecutorImpl.DECL_BINDING;
import static org.apache.sling.pipes.internal.CommandExecutorImpl.DECL_BINDING_CONTENT;
import static org.apache.sling.pipes.internal.CommandExecutorImpl.DECL_BINDING_PATTERN;
import static org.apache.sling.pipes.internal.CommandExecutorImpl.PARAMS_SEPARATOR;
import static org.apache.sling.pipes.CommandUtil.keyValuesToArray;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
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
        commands.enabled = true;
    }

    @Test
    public void getSublist() {
        assertArrayEquals(new String[]{"check", "this"}, commands.getSpaceSeparatedTokens("check this").toArray());
        assertArrayEquals(new String[]{"and", "now","check this"}, commands.getSpaceSeparatedTokens("and now \"check this\"").toArray());
    }

        @Test
    public void testParseTokens() {
        List<CommandExecutorImpl.Token> tokens = commands.parseTokens("some isolated items");
        assertEquals("there should be 1 token", 1, tokens.size());
        CommandExecutorImpl.Token token = tokens.get(0);
        assertEquals("pipe key should be 'some'", "some", token.pipeKey);
        assertEquals("pipe args should be isolated, items", Arrays.asList("isolated", "items"), token.args);
    }
    @Test
    public void testParseTokensWithQuotes() {
        String tokenString = "first arg | second firstarg secondarg @ name second | third blah";
        List<CommandExecutorImpl.Token> tokens = commands.parseTokens(tokenString);
        assertEquals("there should be 3 tokens", 3, tokens.size());
        assertEquals("keys check", Arrays.asList("first","second", "third"), tokens.stream().map(t -> t.pipeKey).collect(Collectors.toList()));
        assertEquals("params check", "second", tokens.get(1).options.name);
    }

    @Test
    public void testParseJson() {
        String tokenString = "json [{\"title\":\"this is the first\", \"path\":\"/content/nested/two\"}, {\"title\":\"this is the second\", \"path\":\"/content/nested/three\"}]";
        List<CommandExecutorImpl.Token> tokens = commands.parseTokens(tokenString);
        assertEquals("there should be 1 token", 1, tokens.size());
        assertEquals("there should be 1 arg", 1, tokens.get(0).args.size());
    }

    @Test
    public void testQuotedTokens() {
        List<CommandExecutorImpl.Token> tokens = commands.parseTokens("some isolated items \"with quotes\"");
        assertEquals("there should be 1 token", 1, tokens.size());
        CommandExecutorImpl.Token token = tokens.get(0);
        assertEquals("pipe args should be isolated, items", Arrays.asList("isolated", "items", "with quotes"), token.args);
    }

    @Test
    public void testSimpleExpression() throws Exception {
        PipeBuilder builder = commands.parse(context.resourceResolver(),"echo /content/fruits");
        assertTrue("there should be a resource", builder.build().getOutput().hasNext());
    }

    @Test
    public void testSimpleChainedConf() throws Exception {
        PipeBuilder builder = commands.parse(context.resourceResolver(),"echo /content/fruits | write some=test key=value");
        assertNotNull("there should be a resource", builder.run());
        ValueMap props = context.currentResource(PATH_FRUITS).getValueMap();
        assertEquals("there should some=test", "test", props.get("some"));
        assertEquals("there should key=value", "value", props.get("key"));
    }

    @Test
    public void testOptions() {
        String expected = "works";
        String optionString = "name works @ path works @ expr works @ with one=works two=works @ outputs one=works two=works";
        CommandExecutorImpl.Options options = commands.getOptions(optionString.split(PARAMS_SEPARATOR));
        assertEquals("check name", expected, options.name);
        assertEquals("check expr", expected, options.expr);
        assertEquals("check path", expected, options.path);
        Map bindings = new HashMap();
        CommandUtil.writeToMap(bindings, true, options.with);
        assertEquals("check with first", expected, bindings.get("one"));
        assertEquals("check with second", expected, bindings.get("two"));
        assertNotNull("a writer should have been created", options.outputs);
    }

    @Test
    public void testOptionsListsWithOneItem() {
        String expected = "works";
        String optionString = "with one=works @ outputs one=works";
        CommandExecutorImpl.Options options = commands.getOptions(optionString.split(PARAMS_SEPARATOR));
        Map bindings = new HashMap();
        CommandUtil.writeToMap(bindings, true, options.with);
        assertEquals("check with first", expected, bindings.get("one"));
        assertNotNull("a writer should have been created", options.outputs);
    }

    @Test
    public void testChainedConfWithInternalOptions() throws Exception {
        PipeBuilder builder = commands.parse(context.resourceResolver(),
        "echo /content/fruits @ name fruits | write some=${path.fruits} key=value");
        assertNotNull("there should be a resource", builder.run());
        ValueMap props = context.currentResource(PATH_FRUITS).getValueMap();
        assertEquals("there should some=/content/fruits", PATH_FRUITS, props.get("some"));
        assertEquals("there should key=value", "value", props.get("key"));
    }

    @Test
    public void testNonBreakingSpaces() throws InvocationTargetException, IllegalAccessException {
        ExecutionResult result = execute("echo /content | mkdir test @ name child");
        assertEquals(1, result.size());
    }

    @Test
    public void adaptToDemoTest() throws Exception {
        String url = "'http://99-bottles-of-beer.net/lyrics.html'";
        String cmd = "egrep " + url + " @ name bottles @ with pattern=(?<number>\\d(\\d)?) | mkdir /var/bottles/${bottles.number}";
        PipeBuilder builder = commands.parse(context.resourceResolver(), cmd);
        ContainerPipe pipe = (ContainerPipe)builder.build();
        ValueMap regexp = pipe.getResource().getChild("conf/bottles").getValueMap();
        assertEquals("we expect expr to be the url", url, regexp.get("expr"));
    }

    @Test
    public void testExecuteWithWriter() throws Exception {
        PipeBuilder builder = plumber.newPipe(context.resourceResolver()).echo("/content/${node}").$("nt:base");
        String path = builder.build().getResource().getPath();
        ExecutionResult result = commands.execute(context.resourceResolver(), path, "outputs title=two['jcr:title'] desc=two['jcr:description']", "with node=fruits");
        assertNotNull(result);
    }

    String testRawServlet(Map<String,Object> params) throws IOException {
        MockSlingHttpServletRequest request = context.request();
        MockSlingHttpServletResponse response = context.response();
        request.setParameterMap(params);
        MockRequestPathInfo pathInfo = (MockRequestPathInfo)request.getRequestPathInfo();
        pathInfo.setExtension("json");
        request.setMethod("POST");
        commands.doPost(request, response);
        if (response.getStatus() != 200) {
            System.out.println(response.getOutputAsString());
        }
        assertEquals(200, response.getStatus());
        return response.getOutputAsString();
    }

    JsonObject testServlet(Map<String,Object> params) throws ServletException, IOException {
        return (JsonObject) JsonUtil.parse(testRawServlet(params));
    }

    @Test
    public void testDisable() throws IOException {
        commands.enabled = false;
        commands.doPost(context.request(), context.response());
        assertEquals(503, context.response().getStatus());
    }

    @Test
    public void testHelp() throws ServletException, IOException {
        Map<String, Object> params = new HashMap<>();
        params.put(CommandExecutorImpl.REQ_PARAM_HELP, "blah");
        String response = testRawServlet(params);
        assertTrue(StringUtils.isNotBlank(response));
    }

    @Test
    public void testSimpleCommandServlet() throws IOException, ServletException {
        Map<String, Object> params = new HashMap<>();
        params.put(CommandExecutorImpl.REQ_PARAM_CMD, "echo /content | mkdir foo | write type=bar");
        JsonObject response = testServlet(params);
        assertEquals(1, response.getJsonNumber("size").intValue());
        assertEquals("/content/foo", response.getJsonArray("items").getString(0));
    }

    @Test
    public void testMultipeLineGetCommandLine() throws IOException, ServletException {
        Map<String, Object> params = new HashMap<>();
        params.put(CommandExecutorImpl.REQ_PARAM_FILE, IOUtils.toString(getClass().getResourceAsStream("/commandsFormats"
            + ".txt"), "UTF-8"));
        MockSlingHttpServletRequest request = context.request();
        request.setParameterMap(params);
        request.setMethod("POST");
        List<String> cmdList = commands.getCommandList(context.request(), new HashMap<>());
        assertEquals(5, cmdList.size());
        for (int i = 0; i < 3; i ++) {
            assertEquals("echo /content | $ /apps/pipes-it/fruit | children nt:unstructured", cmdList.get(i));
        }
        assertEquals ("echo /content | write one=foo nested/two=foo nested/three=foo", cmdList.get(3));
        assertEquals("echo /content | json [{\"title\":\"this is the first\", \"path\":\"/content/nested/two\"}, {\"title\":\"this is the second\", \"path\":\"/content/nested/three\"}] @ name test | echo ${test.path}", cmdList.get(4));
    }

    String[] getItemsArray(JsonObject response) {
        JsonArray jsonItems = response.getJsonArray("items");
        List<String> items = jsonItems.stream().map(o -> ((JsonString)o).getString()).collect(Collectors.toList());
        return items.toArray(new String[jsonItems.size()]);
    }

    JsonObject executeFile(String fileName) throws IOException, ServletException {
        Map<String, Object> params = new HashMap<>();
        params.put(CommandExecutorImpl.REQ_PARAM_FILE, IOUtils.toString(getClass().getResourceAsStream("/" + fileName
                + ".txt"), "UTF-8"));
        return testServlet(params);
    }

    @Test
    public void testChainedCommand() throws IOException, ServletException {
        JsonObject response = executeFile("chainedCommand");
        assertEquals(5, response.getJsonNumber("size").intValue());
        assertArrayEquals(new String[]{ "/content/fruits/banana/isnota/pea","/content/fruits/banana/isnota/carrot",
            "/content/fruits/apple/isnota/pea","/content/fruits/apple/isnota/plum",
            "/content/fruits/apple/isnota/carrot"}, getItemsArray(response));
    }

    @Test
    public void testFileCommandServlet() throws IOException, ServletException {
        JsonObject response = executeFile("testcommand");
        assertEquals(5, response.getJsonNumber("size").intValue());
        assertArrayEquals(new String[]{"/content/beatles/john", "/content/beatles/paul", "/content/beatles/georges",
                "/content/beatles/ringo", "/content/beatles/ringo/jcr:content"}, getItemsArray(response));
    }


    void assertDeclBinding(String input, String expectedBinding, String expectedContent) {
        Matcher matcher = DECL_BINDING_PATTERN.matcher(input);
        assertTrue(input + " should match", matcher.matches());
        assertEquals("binding should be " + expectedBinding, expectedBinding, matcher.group(DECL_BINDING));
        assertEquals("content should be " + expectedContent, expectedContent, matcher.group(DECL_BINDING_CONTENT));
    }

    @Test
    public void testDeclBindingPattern() {
        assertDeclBinding("binding blah = {\"foo\":\"bar\"}", "blah", "{\"foo\":\"bar\"}");
        assertDeclBinding("binding blah={\"foo\":\"bar\"}", "blah", "{\"foo\":\"bar\"}");
        assertDeclBinding("binding blah = {", "blah", "{");
        assertDeclBinding("binding csvStart = name,title", "csvStart", "name,title");
        assertDeclBinding("binding csvStart =", "csvStart", "");
    }


    void assertSpaceSeparatedTokens(String input, String... tokens) {
        assertArrayEquals(tokens,  commands.getSpaceSeparatedTokens(input).toArray());
    }
    @Test
    public void getSpaceSeparatedToken() {
        assertSpaceSeparatedTokens("rm","rm");
        assertSpaceSeparatedTokens("write foo=bar a=b", "write", "foo=bar", "a=b");
        assertSpaceSeparatedTokens("check \"foo bar\"", "check", "foo bar");
        assertSpaceSeparatedTokens("write prop=\"foo bar\"","write", "prop=\"foo bar\"");
    }

    @Test
    public void testDeclaredBindingBasedInit() throws ServletException, IOException {
        JsonObject response = executeFile("declbasedinit");
        assertEquals(4, response.getJsonNumber("size").intValue());
        assertArrayEquals(new String[]{"/content/pages/1/leaves/child1","/content/pages/1/leaves/child2",
                "/content/pages/2/leaves/child1","/content/pages/2/leaves/child2"}, getItemsArray(response));
    }
}