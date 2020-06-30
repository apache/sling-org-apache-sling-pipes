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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jsoup.Jsoup;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class PlumberServletIT extends PipesTestSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlumberServletIT.class);

    /**
     * Java type for parsing the json
     */
    class ExpectedResponse {
        int size;
        int nbErrors;
        ArrayList<String> items;
        ArrayList<String> errors;
    }

    @Test
    public void testListComponentJson() throws IOException {
        final String url = String.format("http://localhost:%s/etc/pipes-it/another-list.json", httpPort());
        LOGGER.info("fetching {}", url);
        final String response = Jsoup.connect(url).header("Authorization", basicAuthorizationHeader(ADMIN_CREDENTIALS)).ignoreContentType(true).execute().body();
        LOGGER.info("retrieved following response {}", response);
        final ExpectedResponse json = (new Gson()).fromJson(response, new TypeToken<ExpectedResponse>(){}.getType());
        assertEquals("there should be 2 elements", 2, json.size);
        assertArrayEquals("should be fruits array", new String[] {"/content/fruits/apple", "/content/fruits/banana"}, json.items.toArray());
    }

    @Test
    @Ignore
    public void testErrors() throws IOException {
        final String url = String.format("http://localhost:%s/etc/pipes-it/bad-list.json", httpPort());
        LOGGER.info("fetching {}", url);
        final String response = Jsoup.connect(url).header("Authorization", basicAuthorizationHeader(ADMIN_CREDENTIALS)).ignoreContentType(true).execute().body();
        LOGGER.info("retrieved following response {}", response);
        final ExpectedResponse json = (new Gson()).fromJson(response, new TypeToken<ExpectedResponse>(){}.getType());
        assertEquals("there should be 1 elements", 1, json.size);
        assertArrayEquals("should be single apple array", new String[] {"/content/fruits/apple"}, json.items.toArray());
        assertEquals("there should be 1 error", 1, json.nbErrors);
        assertArrayEquals("should be single error array", new String[] {"${name.list === 'apple' ? path.apple : unexistingVariable}"}, json.errors.toArray());
    }

}

