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

import org.apache.commons.io.IOUtils;
import org.apache.sling.pipes.internal.JsonWriter;
import org.apache.sling.pipes.internal.JsonUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonObject;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class PlumberServletIT extends PipesTestSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(PipeModelIT.class);

    @Test
    public void testListComponentJson() throws IOException {
        final String urlString = String.format("http://localhost:%s/etc/pipes-it/another-list.json", httpPort());
        LOGGER.info("fetching {}", urlString);
        URL url = new URL(urlString);
        StringWriter writer = new StringWriter();
        IOUtils.copy(url.openStream(), writer, Charset.defaultCharset());
        String response = writer.toString();
        LOGGER.info("retrieved following response {}", response);
        JsonObject main = JsonUtil.parseObject(response);
        assertTrue("there should be an items key", main.containsKey(JsonWriter.KEY_ITEMS));
        assertTrue("there should be a size key", main.containsKey(JsonWriter.KEY_SIZE));
        assertEquals("there should be 2 elements", 2, main.getInt(JsonWriter.KEY_SIZE));
    }

}

