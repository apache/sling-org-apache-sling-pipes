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

import java.util.Map;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;

public class JxltEngine {

    JexlEngine jexl;
    JexlContext jc;
    static final String KEY_TIME = "timeutil";

    public JxltEngine(Map<String, Object> context) {
        jexl = new JexlBuilder().create();
        jc = new MapContext(context);
        jc.set(KEY_TIME, new TimeUtil());
    }

    public Object parse(String expression) {
        JexlExpression e = jexl.createExpression(expression);
        return e.evaluate(jc);
    }
}
