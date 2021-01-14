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

import org.apache.sling.pipes.AbstractPipeTest;
import org.junit.Test;

import java.util.Calendar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TimeUtilTest extends AbstractPipeTest {

    @Test
    public void testOfDate() {
        TimeUtil timeUtil = new TimeUtil();
        Calendar cal = timeUtil.ofDate("2012-12-02");
        assertNotNull(cal);
        assertEquals(2012, cal.get(Calendar.YEAR));
    }

    @Test
    public void testOf() {
        TimeUtil timeUtil = new TimeUtil();
        Calendar cal = timeUtil.of("2012-12-02T12:30:20+02:00");
        assertNotNull(cal);
        assertEquals(2012, cal.get(Calendar.YEAR));
        assertNotNull(timeUtil.of("2012-12-30T09:20:26Z"));
    }
}