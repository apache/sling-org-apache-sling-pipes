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
package org.apache.sling.pipes.internal.inputstream;

import org.apache.commons.io.IOUtils;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.AbstractInputStreamPipe;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pipe outputting matches of a regexp from a plain text file
 */
public class RegexpPipe extends AbstractInputStreamPipe {
    private static Logger logger = LoggerFactory.getLogger(RegexpPipe.class);

    public static final String RESOURCE_TYPE = "slingPipes/egrep";
    public static final String PN_PATTERN = "pattern";
    private static final Pattern PATTERN_NAME = Pattern.compile("\\?<([\\w]+)>");
    private static final short PATTERN_IDX_NAME = 1;

    public RegexpPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
    }

    @Override
    public Iterator<Resource> getOutput(InputStream inputStream) {
        Iterator<Resource> output = EMPTY_ITERATOR;
        try {
            String patternString = bindings.instantiateExpression(properties.get(PN_PATTERN, String.class));
            if (patternString == null){
                logger.debug("pattern {} evaluates as empty.", properties.get(PN_PATTERN, String.class));
                return output;
            }
            final Collection<String> names = getGroupNames(patternString);
            if (names.isEmpty()){
                logger.debug("no name defined, will take the whole match");
            }
            Pattern pattern = Pattern.compile(patternString);
            String text = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            logger.trace("about to parse {}", text);
            Matcher matcher = pattern.matcher(text);
            if (matcher.find()) {
                final Resource next = getInput();
                output = new Iterator<Resource>() {
                    boolean hasNext = true;
                    @Override
                    public boolean hasNext() {
                        return hasNext;
                    }

                    @Override
                    public Resource next() {
                        if (! hasNext) {
                            throw new NoSuchElementException();
                        }
                        if (!names.isEmpty()){
                            Map<String, Object> map = new HashMap<>();
                            for (String name : names) {
                                map.put(name, matcher.group(name));
                            }
                            binding = map;
                        } else {
                            //no group names defined, we take the whole match
                            binding = matcher.group(0);
                        }
                        hasNext = matcher.find();
                        return next;
                    }
                };
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
        return output;
    }

    /**
     * @param pattern configured pattern
     * @return list of group names identified in a given pattern
     */
    protected Collection<String> getGroupNames(String pattern){
        Collection<String> names = new ArrayList<>();
        Matcher nameMatcher = PATTERN_NAME.matcher(pattern);
        while (nameMatcher.find()){
            names.add(nameMatcher.group(PATTERN_IDX_NAME));
        }
        return names;
    }


}
