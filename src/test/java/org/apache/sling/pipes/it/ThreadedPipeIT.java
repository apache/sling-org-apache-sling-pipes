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
package org.apache.sling.pipes.it;

import org.apache.sling.api.resource.ResourceResolver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

/**
 * testing executor with dummy child pipes
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class ThreadedPipeIT extends PipesTestSupport {

    public static final String NN_DEFAULT = "defaultExecutor";
    public static final String NN_STRAINED = "strainedExecutor";

    @Test
    public void testWithDefaults() throws Exception {
        try (ResourceResolver resolver = resolver()) {
            plumber.newPipe(resolver)
                .ref("")
                .runParallel(7);
        }

    }

    @Test
    public void testStrained() throws Exception {
//        Iterator<Resource> tenPipes = getOutput(PATH_PIPE + "/" + NN_STRAINED);
//        int numResults = 0;
//        while (tenPipes.hasNext()) {
//             assumeNotNull("The output should not have null", tenPipes.next());
//             numResults++;
//        }
//        assertEquals("All the sub-pipes output should be present exactly once in Executor output", 10*6, numResults);
    }
}