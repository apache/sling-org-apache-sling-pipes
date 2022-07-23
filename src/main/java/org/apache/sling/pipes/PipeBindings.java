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
package org.apache.sling.pipes;

import org.apache.commons.io.IOUtils;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Execution bindings of a pipe, and all expression related
 */
public class PipeBindings {

    private static final Logger log = LoggerFactory.getLogger(PipeBindings.class);

    public static final String NN_ADDITIONALBINDINGS = "additionalBindings";

    public static final String PN_ADDITIONALSCRIPTS = "additionalScripts";

    public static final String NN_PROVIDERS = "providers";

    public static final String PN_ENGINE = "engine";

    public static final String FALSE_BINDING = "${false}";

    /**
     * add ${path.pipeName} binding allowing to retrieve pipeName's current resource path
     */
    public static final String PATH_BINDING = "path";

    public static final String CACONFIG_BINDING = "caconfig";

    /**
     * add ${name.pipeName} binding allowing to retrieve pipeName's current resource name
     */
    public static final String NAME_BINDING = "name";

    public static final String INJECTED_SCRIPT_REGEXP = "\\$\\{(([^\\{^\\}]+(\\{\\{)?(\\}\\})?(\\{[0-9,]+\\})?)+)\\}";
    private static final Pattern INJECTED_SCRIPT = Pattern.compile(INJECTED_SCRIPT_REGEXP);
    protected static final String IF_PREFIX = "$if";
    protected static final Pattern CONDITIONAL_STRING =  Pattern.compile("^\\" + IF_PREFIX + INJECTED_SCRIPT_REGEXP);

    ScriptEngine engine;
    
    ScriptContext scriptContext = new SimpleScriptContext();

    Map<String, String> pathBindings = new HashMap<>();

    Map<String, String> nameBindings = new HashMap<>();

    Map<String, Map<String, Object>> caconfigBindings = new HashMap<>();

    Map<String, Resource> outputResources = new HashMap<>();

    boolean allowAdditionalScripts = true;

    String currentError;

    Plumber plumber;

    public PipeBindings(@NotNull Plumber plumber, @NotNull Resource resource) {
        this(resource, plumber.allowAdditionalScripts());
        this.plumber = plumber;
    }

    /**
     * public constructor, built from pipe's resource
     * @param resource pipe's configuration resource
     */
    public PipeBindings(@NotNull Resource resource) {
        this(resource, false);
    }

    /**
     * public constructor, built from pipe's resource
     * @param resource pipe's configuration resource
     * @param allowAdditionalScripts flag allowing additional scripts
     */
    public PipeBindings(@NotNull Resource resource, boolean allowAdditionalScripts) {
        this.allowAdditionalScripts = allowAdditionalScripts;
    	//Setup script engines
        String engineName = resource.getValueMap().get(PN_ENGINE, String.class);
        if (StringUtils.isNotBlank(engineName)) {
            initializeScriptEngine(engineName);
        }
    	
        //add path bindings where path.MyPipe will give MyPipe current resource path
        getBindings().put(PATH_BINDING, pathBindings);

        //add name bindings where name.MyPipe will give MyPipe current resource name
        getBindings().put(NAME_BINDING, nameBindings);

        //add caconfig bindings where caconfig.MyPipe will give MyPipe current ca configuration
        getBindings().put(CACONFIG_BINDING, caconfigBindings);
    }

    /**
     * @param value
     * @return script equivalent
     */
    public static String embedAsScript(String value) {
        return "${" + value + "}";
    }

    /**
     * add a binding
     * @param name binding's name
     * @param value binding's value
     */
    public void addBinding(String name, Object value){
        log.debug("Adding binding {}={}", name, value);
        getBindings().put(name, value);
    }

    /**
     * adds additional bindings (global variables to use in child pipes expressions)
     * @param bindings key/values bindings to add to the existing bindings
     */
    public void addBindings(Map<String, Object> bindings) {
        log.info("Adding bindings {}", bindings);
        getBindings().putAll(bindings);
    }

    /**
     * add a script file to the engine
     * @param resolver resolver with which the file should be read
     * @param path path of the script file
     */
    public void addScript(ResourceResolver resolver, String path) {
        if (!allowAdditionalScripts) {
            throw new SecurityException("additional scripts are not allowed per configuration");
        }
        InputStream is = null;
        try {
            if (path.startsWith("http")) {
                try {
                    URL remoteScript = new URL(path);
                    is = remoteScript.openStream();
                } catch (Exception e) {
                    log.error("unable to retrieve remote script", e);
                }
            } else if (path.startsWith("/")) {
                Resource scriptResource = resolver.getResource(path);
                if (scriptResource != null) {
                    is = scriptResource.adaptTo(InputStream.class);
                }
            }
            if (is != null) {
                try {
                    getEngine().eval(new InputStreamReader(is), scriptContext);
                } catch (Exception e) {
                    log.error("Add script: unable to evaluate script {}", path, e);
                }
            }
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    private int processMatcher(int start, String expr, StringBuilder expression, Matcher matcher) {
        if (matcher.start() > start) {
            if (expression.length() == 0) {
                expression.append("'");
            }
            expression.append(expr, start, matcher.start());
        }
        if (expression.length() > 0) {
            expression.append("' + ");
        }
        expression.append(matcher.group(1));
        start = matcher.end();
        if (start < expr.length()) {
            expression.append(" + '");
        }
        return start;
    }

    /**
     * Doesn't look like nashorn likes template strings :-(
     * @param expr ECMA like expression <code>blah${'some' + 'ecma' + 'expression'}</code>
     * @return computed expression, null if the expression is a plain string
     */
    String computeTemplateExpression(String expr) {
        Matcher matcher = INJECTED_SCRIPT.matcher(expr);
        if (INJECTED_SCRIPT.matcher(expr).find()) {
            StringBuilder expression = new StringBuilder();
            int start = 0;
            while (matcher.find()) {
                start = processMatcher(start, expr, expression, matcher);
            }
            if (start < expr.length()) {
                expression.append(expr.substring(start) + "'");
            }
            return expression.toString();
        }
        return null;
    }

    private ScriptEngine getEngine() {
        if (engine == null && getBindings().containsKey(PN_ENGINE)){
            initializeScriptEngine((String) getBindings().get(PN_ENGINE));
        }
        return engine;
    }

    /**
     * evaluate a given expression
     * @param expr ecma like expression
     * @return object that is the result of the expression
     */
    protected Object evaluate(String expr) {
        try {
            String computed = computeTemplateExpression(expr);
            if (computed != null) {
                return getEngine() != null ? engine.eval(computed, scriptContext) :
                        plumber.evaluate(computed, getBindings());
            }
        } catch (ScriptException | JexlException e) {
            throw new IllegalArgumentException(e);
        }
        return expr;
    }

    /**
     * Instantiate object from expression
     * @param expr ecma expression
     * @return instantiated object
     */
    public Object instantiateObject(String expr) {
        return evaluate(expr);
    }

    /**
     * return registered bindings
     * @return bindings
     */
    public Bindings getBindings() {
        return scriptContext.getBindings(ScriptContext.ENGINE_SCOPE);
    }
    
    /**
     * return Pipe <code>name</code>'s F binding
     * @param name name of the pipe
     * @return resource corresponding to that pipe output
     */
    public Resource getExecutedResource(String name) {
        return outputResources.get(name);
    }

    /**
     * Initialize the ScriptEngine.
     * In some contexts the nashorn engine cannot be obtained from thread's class loader. Do fallback to system classloader.
     * @param engineName name of the engine as registered in the JVM
     */
    public void initializeScriptEngine(String engineName) {
        engine = new ScriptEngineManager().getEngineByName(engineName);
        if(engine == null){
            //Fallback to system classloader
            engine = new ScriptEngineManager(null).getEngineByName(engineName);
            //Check if engine can still not be instantiated
            if(engine == null){
                throw new IllegalArgumentException("Can not instantiate " + engineName + " scriptengine. Check JVM version & capabilities.");
            }
        }
        engine.setContext(scriptContext);
    }

    /**
     * Return expression, instantiated expression or null if the expression is conditional and evaluation is falsy
     * @param conditionalExpression can be static, or dynamic, can be conditional in which case it must be of following
     * format <code>$if${condition}someString</code>. someString will be returned if condition is true, otherwise null
     * @return instantiated expression or null if expression is conditional (see above) and condition is falsy
     */
    public String conditionalString(String conditionalExpression) {
        Matcher matcher = CONDITIONAL_STRING.matcher(conditionalExpression);
        if (matcher.find()){
            Object output = evaluate(StringUtils.substringAfter(matcher.group(0), IF_PREFIX));
            if (output != null){
                String s = output.toString().toLowerCase().trim();
                if(StringUtils.isNotEmpty(s) && !"false".equals(s) && !"undefined".equals(s)){
                    return instantiateExpression(conditionalExpression.substring(matcher.group(0).length()));
                }
            }
        } else {
            return instantiateExpression(conditionalExpression);
        }
        return null;
    }

    /**
     * Expression is a function of variables from execution context, that
     * we implement here as a String
     * @param expr ecma like expression
     * @return String that is the result of the expression
     */
    public String instantiateExpression(String expr) {
        Object obj = evaluate(expr);
        return obj != null ? obj.toString() : null;
    }

    /**
     * check if a given bindings is defined or not
     * @param name name of the binding
     * @return true if <code>name</code> is registered
     */
    public boolean isBindingDefined(String name){
        return getBindings().containsKey(name);
    }

    /**
     * Update current resource of a given pipe, and appropriate binding
     * @param pipe pipe we'll extract the output binding from
     * @param resource current resource in the pipe execution
     */
    public void updateBindings(Pipe pipe, Resource resource) {
        outputResources.put(pipe.getName(), resource);
        updateStaticBindings(pipe.getName(), resource);
        addBinding(pipe.getName(), pipe.getOutputBinding());
    }

    /**
     * Update all the static bindings related to a given resource
     * @param name name under which static bindings should be recorded
     * @param resource resource from which static bindings will be built
     */
    public void updateStaticBindings(String name, Resource resource){
        if (resource != null) {
            pathBindings.put(name, resource.getPath());
            nameBindings.put(name, resource.getName());
            if (plumber != null) {
                caconfigBindings.put(name, plumber.getContextAwareConfigurationMap(resource));
            }
        }
    }

    /**
     * @return current error if any, and reset it
     */
    public String popCurrentError() {
        String returnValue = currentError;
        currentError = null;
        return returnValue;
    }

    /**
     * @param currentError error path to set
     */
    public void setCurrentError(String currentError) {
        this.currentError = currentError;
    }
}
