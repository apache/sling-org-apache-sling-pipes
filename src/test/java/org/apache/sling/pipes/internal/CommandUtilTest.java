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

import junit.framework.TestCase;
import org.junit.Test;

public class CommandUtilTest extends TestCase {

    @Test
    public void testEmbedIfNeeded() {
        assertEquals(2, CommandUtil.embedIfNeeded(2));
        assertEquals(true, CommandUtil.embedIfNeeded(true));
        assertEquals("/path/left/0/un-touc_hed", CommandUtil.embedIfNeeded("/path/left/0/un-touc_hed"));
        assertEquals("/content/json/array/${json.test}", CommandUtil.embedIfNeeded("/content/json/array/${json.test}"));
        assertEquals("${vegetables['jcr:title']}", CommandUtil.embedIfNeeded("vegetables['jcr:title']"));
        assertEquals("${new Date(\"2018-05-05T11:50:55\")}", CommandUtil.embedIfNeeded("new Date(\"2018-05-05T11:50:55\")"));
        assertEquals("${some + wellformed + script}", CommandUtil.embedIfNeeded("${some + wellformed + script}"));
        assertEquals("${['one','two']}", CommandUtil.embedIfNeeded("['one','two']"));
        assertEquals("${true}", CommandUtil.embedIfNeeded("true"));
        assertEquals("${'some string'}", CommandUtil.embedIfNeeded("'some string'"));
    }
}