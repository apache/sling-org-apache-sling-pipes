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

import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.PipeBindings;
import org.apache.sling.pipes.Plumber;

/**
 * specific reference pipe, to be used with dynamic reference usage, specifically recursive ones
 */
public class ShallowReferencePipe extends ReferencePipe {
    public static final String RESOURCE_TYPE = "slingPipes/shallow";

    public ShallowReferencePipe(Plumber plumber, Resource resource, PipeBindings upperBindings) {
        super(plumber, resource, upperBindings);
    }

    @Override
    public boolean modifiesContent() {
        //we assume some other pipes will take care of that status
        return false;
    }

    @Override
    public void before() {
        //we don't want anything to be executed before
    }

    @Override
    public void after() {
        //we don't want anything to be executed after
    }
}
