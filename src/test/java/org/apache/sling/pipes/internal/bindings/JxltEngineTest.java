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
package org.apache.sling.pipes.internal.bindings;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.apache.sling.pipes.AbstractPipeTest;
import org.junit.Before;
import org.junit.Test;

public class JxltEngineTest extends AbstractPipeTest {

    JxltEngine engine;
    @Before
    public void setup() {
        HashMap map = new HashMap();
        map.put("shallow", true);
        HashMap<String, Object> childMap = new HashMap<>();
        childMap.put("depth.numeric", 1);
        childMap.put("shallow", false);
        HashMap<String, Object> grandChildMap = new HashMap<>();
        grandChildMap.put("level", "two");
        childMap.put("grandChild", grandChildMap);
        map.put("child", childMap);
        engine = new JxltEngine(map);
    }

    @Test
    public void testParseBoolean() {
        assertTrue((Boolean)engine.parse("shallow"));
        assertFalse((Boolean)engine.parse("child.shallow"));
        assertTrue((Boolean)engine.parse("child.shallow||shallow"));
        assertFalse((Boolean)engine.parse("child.shallow&&shallow"));
    }


    @Test
    public void testTernary() {
        assertEquals(1, engine.parse("shallow?1:2"));
        assertEquals("two", engine.parse("child.shallow?'one':'two'"));
    }
}
