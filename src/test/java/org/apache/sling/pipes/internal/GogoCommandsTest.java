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

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.PipeBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class GogoCommandsTest extends AbstractPipeTest {

    GogoCommands commands;

    @Before
    public void setup() throws PersistenceException {
        super.setup();
        commands = new GogoCommands();
        commands.plumber = plumber;
    }

    @Test
    public void testParseTokens(){
        List<GogoCommands.Token> tokens = commands.parseTokens("some", "isolated", "items");
        assertEquals("there should be 1 token", 1, tokens.size());
        GogoCommands.Token token = tokens.get(0);
        assertEquals("pipe key should be 'some'","some", token.pipeKey);
        assertEquals("pipe args should be isolated, items", Arrays.asList("isolated","items"), token.args);
        String tokenString = "first arg / second firstarg secondarg @ name second / third blah";
        tokens = commands.parseTokens(tokenString.split("\\s"));
        assertEquals("there should be 3 tokens", 3, tokens.size());
        assertEquals("keys check", Arrays.asList("first","second", "third"), tokens.stream().map(t -> t.pipeKey).collect(Collectors.toList()));
        assertEquals("params check", "second", tokens.get(1).options.name);
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
        GogoCommands.Options options = commands.getOptions(optionString.split("\\s"));
        assertEquals("check name", expected, options.name);
        assertEquals("check expr", expected, options.expr);
        assertEquals("check path", expected, options.path);
        Map bindings = new HashMap();
        CommandUtil.writeToMap(bindings, options.bindings);
        assertEquals("check bindings first", expected, bindings.get("one"));
        assertEquals("check bindings second", expected, bindings.get("two"));
        assertNotNull("a writer should have been created", options.writer);
        Map outputs = options.writer.getCustomOutputs();
        assertEquals("check writer first", expected, outputs.get("one"));
        assertEquals("check writer second", expected, outputs.get("two"));
    }

    @Test
    public void testOptionsListsWithOneItem() {
        String expected = "works";
        String optionString = "@ with one=works @ outputs one=works";
        GogoCommands.Options options = commands.getOptions(optionString.split("\\s"));
        Map bindings = new HashMap();
        CommandUtil.writeToMap(bindings, options.bindings);
        assertEquals("check bindings first", expected, bindings.get("one"));
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
    public void testExecuteWithWriter() throws Exception {
        PipeBuilder builder = plumber.newPipe(context.resourceResolver()).echo("/content/${node}").$("nt:base");
        String path = builder.build().getResource().getPath();
        ExecutionResult result = commands.executeInternal(context.resourceResolver(), path, "@ outputs title=jcr:title desc=jcr:description @ with node=fruits");
    }
}