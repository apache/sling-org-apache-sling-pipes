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

import org.apache.sling.api.resource.Resource;
import org.apache.sling.caconfig.ConfigurationBuilder;
import org.apache.sling.caconfig.spi.ConfigurationMetadataProvider;
import org.apache.sling.caconfig.spi.metadata.ConfigurationMetadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This is a "virtual" containing configuration names as keys, and the underlying value maps/value map collections as values.
 * The map accesses only the data that is really required in a lazy fashion.
 */
public class ConfigurationMap implements Map<String, Object> {

    private final Resource resource;
    private final ConfigurationMetadataProvider configMetadataProvider;
    private Set<String> configNamesCache;
    private Map<String, Object> valuesCache = new HashMap<>();

    public ConfigurationMap(Resource resource, ConfigurationMetadataProvider provider) {
        this.resource = resource;
        this.configMetadataProvider = provider;
    }

    private Set<String> getConfigNames() {
        if (configNamesCache == null) {
            configNamesCache = configMetadataProvider.getConfigurationNames();
        }
        return configNamesCache;
    }

    @Override
    public int size() {
        return getConfigNames().size();
    }

    @Override
    public boolean isEmpty() {
        return getConfigNames().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return getConfigNames().contains(key);
    }

    @Override
    public Object get(Object key) {
        Object value = valuesCache.get(key);
        if (value == null) {
            value = getConfigValue((String) key);
            if (value != null) {
                valuesCache.put((String) key, value);
            }
        }
        return value;
    }

    private Object getConfigValue(String configName) {
        @SuppressWarnings("null")
        ConfigurationBuilder configBuilder = resource.adaptTo(ConfigurationBuilder.class).name(configName);
        if (isCollection(configName)) {
            return configBuilder.asValueMapCollection();
        } else {
            return configBuilder.asValueMap();
        }
    }

    private boolean isCollection(String configName) {
        ConfigurationMetadata configMetadata = configMetadataProvider.getConfigurationMetadata(configName);
        if (configMetadata != null) {
            return configMetadata.isCollection();
        } else {
            return false;
        }
    }

    @Override
    public Set<String> keySet() {
        return getConfigNames();
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object put(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Object> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        throw new UnsupportedOperationException();
    }
}
