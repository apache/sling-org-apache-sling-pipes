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
import org.apache.sling.pipes.AbstractPipeTest;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PlumberImplTest extends AbstractPipeTest {

    @Test
    public void testPurge() throws InvocationTargetException, IllegalAccessException, PersistenceException {
        execute("mkdir /var/pipes/this/is/going/away | write statusModified=timeutil.of('2018-05-05T11:50:55+01:00')");
        execute("mkdir /var/pipes/this/is/also/going/away| write statusModified=timeutil.of('2018-05-05T11:50:55+01:00')");
        String recentDate =  Instant.now().minus(15, ChronoUnit.DAYS).toString();
        execute("mkdir /var/pipes/this/should/stay| write statusModified=timeutil.of('" + recentDate + "')");
        assertNotNull("checking that part of the tree has been created", context.resourceResolver().getResource("/var/pipes/this/is"));
        ((PlumberImpl)plumber).purge(context.resourceResolver(), Instant.now(), 30);
        assertNull("there should be no more /var/pipes/this/is resource", context.resourceResolver().getResource("/var/pipes/this/is"));
        assertNotNull("there should still be /var/pipes/this/should/stay resource", context.resourceResolver().getResource("/var/pipes/this/should/stay"));
    }
}