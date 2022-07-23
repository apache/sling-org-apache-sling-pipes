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

import static org.apache.sling.pipes.CommandUtil.CONFIGURATION_PATTERN;
import static org.apache.sling.pipes.CommandUtil.FIRST_KEY;
import static org.apache.sling.pipes.CommandUtil.SECOND_KEY;
import static org.apache.sling.pipes.CommandUtil.keyValuesToArray;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.sling.pipes.AbstractPipeTest;
import org.apache.sling.pipes.CommandUtil;
import org.junit.Assert;
import org.junit.Test;

public class CommandUtilTest extends AbstractPipeTest {


    private void assertMatch(Pattern pattern, String token, String first, String second) {
        Matcher matcher = pattern.matcher(token);
        assertTrue(pattern.toString() + " should match with " + token, matcher.matches());
        assertEquals("first token should be " + first, first, matcher.group(FIRST_KEY));
        assertEquals("second token should be " + second, second, matcher.group(SECOND_KEY));
    }
    @Test
    public void testTokenMatch() {
        assertMatch(CONFIGURATION_PATTERN, "foo=bar", "foo", "bar");
        assertMatch(CONFIGURATION_PATTERN, "${foo}=bar", "${foo}", "bar");
        assertMatch(CONFIGURATION_PATTERN, "foo=${bar}", "foo", "${bar}");
        assertMatch(CONFIGURATION_PATTERN, "foo='bar'", "foo", "'bar'");
        assertMatch(CONFIGURATION_PATTERN, "foo=${'bar'}", "foo", "${'bar'}");
        assertMatch(CONFIGURATION_PATTERN, "${foo == bar ? 1 : 2}=bar", "${foo == bar ? 1 : 2}", "bar");
        assertMatch(CONFIGURATION_PATTERN, "foo=${foo == bar ? 1 : 2}", "foo", "${foo == bar ? 1 : 2}");
        assertMatch(CONFIGURATION_PATTERN, "foo/bar=check/some", "foo/bar", "check/some");
        assertMatch(CONFIGURATION_PATTERN, "foo:bar='.+'", "foo:bar", "'.+'");
    }
    @Test
    public void testEmbedIfNeeded() {
        assertEquals(2, CommandUtil.embedIfNeeded(2));
        assertEquals(true, CommandUtil.embedIfNeeded(true));
        assertEquals("/path/left/0/un-touc_hed", CommandUtil.embedIfNeeded("/path/left/0/un-touc_hed"));
        assertEquals("/content/json/array/${json.test}", CommandUtil.embedIfNeeded("/content/json/array/${json.test}"));
        assertEquals("${vegetables['jcr:title']}", CommandUtil.embedIfNeeded("vegetables['jcr:title']"));
        assertEquals("${new Date(\"2018-05-05T11:50:55\")}", CommandUtil.embedIfNeeded("new Date(\"2018-05-05T11:50:55\")"));
        assertEquals("${some + wellformed + script}", CommandUtil.embedIfNeeded("${some + wellformed + script}"));
        assertEquals("${true}", CommandUtil.embedIfNeeded("true"));
        assertEquals("${'some string'}", CommandUtil.embedIfNeeded("'some string'"));
        assertEquals("${['one','two']}", CommandUtil.embedIfNeeded("['one','two']"));
    }

    @Test
    public void testHandleMixin() {
        String[] expected = new String[] {"rep:versionable", "rep:AccessControllable"};
        Assert.assertArrayEquals(expected, CommandUtil.handleMixins("[ rep:versionable, rep:AccessControllable]"));
        Assert.assertArrayEquals(expected, CommandUtil.handleMixins("[rep:versionable,rep:AccessControllable]"));
    }

    @Test
    public void testWriteToMap() {
        Map<String, Object> map = new HashMap<>();
        CommandUtil.writeToMap(map, true, "p1", "'some string'", "p2", "/some/path",
                "p3","['one','two']", "jcr:mixinTypes", "[ rep:versionable, some:OtherMixin]");
        assertEquals("${'some string'}", map.get("p1"));
        assertEquals("/some/path", map.get("p2"));
        assertEquals("${['one','two']}", map.get("p3"));
        Assert.assertArrayEquals(new String [] {"rep:versionable", "some:OtherMixin"}, (String[])map.get("jcr:mixinTypes"));
    }

    @Test
    public void testTrimQuotes() {
        assertEquals("blah", CommandUtil.trimQuotes("\"blah\""));
        assertEquals("blah", CommandUtil.trimQuotes("blah"));
    }

    @Test
    public void testKeyValuesToArray() {
        assertArrayEquals(new String[] {"foo","bar","another","with spaces "}, keyValuesToArray(Arrays.asList("foo=bar","another=\"with spaces \"")));
        assertArrayEquals("1", new String[]{"one","two","three","four"}, keyValuesToArray(Arrays.asList("one=two","three=four")));
        assertArrayEquals("2", new String[]{"one","two","three","${four}"}, keyValuesToArray(Arrays.asList("one=two","three=${four}")));
        assertArrayEquals("3", new String[]{"${one == 'check'? 'three':'four'}","two"}, keyValuesToArray((Arrays.asList("${one == 'check'? 'three':'four'}=two"))));
        assertArrayEquals("4", new String[]{"a_b-c","two"}, keyValuesToArray((Arrays.asList("a_b-c=two"))));
        assertArrayEquals("5", new String[]{"one","two","three","${four == 'blah' ? 'five' : 'six'}"},
                keyValuesToArray(Arrays.asList("one=two","three=${four == 'blah' ? 'five' : 'six'}")));
        assertArrayEquals("6", new String[]{"jcr:content/singer","${'ringo' == one ? false : true}"}, keyValuesToArray(Arrays.asList("jcr:content/singer=${'ringo' == one ? false : true}")));
        assertArrayEquals("7", new String[]{"jcr:content/tag","${text.replaceAll('foo','<div class=\"foo\">foo</div>'}"}, keyValuesToArray(Arrays.asList("jcr:content/tag=\"${text.replaceAll('foo','<div class_EQ_\"foo\">foo</div>'}\"")));
    }

}