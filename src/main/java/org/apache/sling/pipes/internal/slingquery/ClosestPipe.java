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
package org.apache.sling.pipes.internal.slingquery;

import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.query.SlingQuery;

import static org.apache.sling.query.SlingQuery.$;

public class ClosestPipe extends AbstractExpressionSlingQueryPipe {
    public static final String RESOURCE_TYPE = RT_PREFIX + "closest";

    public ClosestPipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
    }

    @Override
    protected SlingQuery getQuery(Resource resource, String expression) {
        return $(resource).closest(expression);
    }
}
