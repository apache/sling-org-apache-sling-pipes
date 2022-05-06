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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.request.RequestParameter;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.servlets.ServletResolverConstants;
import org.apache.sling.pipes.CommandExecutor;
import org.apache.sling.pipes.CommandUtil;
import org.apache.sling.pipes.ExecutionResult;
import org.apache.sling.pipes.OutputWriter;
import org.apache.sling.pipes.Pipe;
import org.apache.sling.pipes.PipeBuilder;
import org.apache.sling.pipes.PipeExecutor;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.pipes.internal.inputstream.JsonPipe;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_NOT_ACCEPTABLE;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.sling.pipes.CommandUtil.keyValuesToArray;
import static org.apache.sling.pipes.CommandUtil.writeToMap;

import javax.json.JsonException;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServletResponse;

@Component(service = {Servlet.class, CommandExecutor.class}, property= {
    ServletResolverConstants.SLING_SERVLET_RESOURCE_TYPES + "=" + CommandExecutorImpl.RESOURCE_TYPE,
    ServletResolverConstants.SLING_SERVLET_METHODS + "=POST",
    ServletResolverConstants.SLING_SERVLET_EXTENSIONS + "=json",
    ServletResolverConstants.SLING_SERVLET_EXTENSIONS + "=csv"
})
@Designate(ocd = CommandExecutorImpl.Configuration.class)
public class CommandExecutorImpl extends AbstractPlumberServlet implements CommandExecutor {
    final Logger log = LoggerFactory.getLogger(CommandExecutorImpl.class);
    public static final String RESOURCE_TYPE = "slingPipes/exec";
    static final String REQ_PARAM_FILE = "pipe_cmdfile";
    static final String REQ_PARAM_CMD = "pipe_cmd";
    static final String REQ_PARAM_HELP = "pipe_help";
    static final String CMD_LINE_PREFIX = "cmd_line_";
    static final String PN_DESCRIPTION = "commandParsed";

    static final String TOKEN = "token";
    static final String WHITE_SPACE_SEPARATOR = "[\\s\\h]";
    static final String COMMENT_PREFIX = "#";
    static final String SEPARATOR = "|";
    static final String PIPE_SEPARATOR = WHITE_SPACE_SEPARATOR + "*\\" + SEPARATOR + WHITE_SPACE_SEPARATOR + "*";
    static final String LINE_SEPARATOR = " ";
    static final String PARAMS = "@";
    static final List<String> JSON_EXPR_KEYS = Arrays.asList(JsonPipe.JSON_KEY);
    static final String JSON_START = "\"[{";
    static final String PARAMS_SEPARATOR = WHITE_SPACE_SEPARATOR + "+" + PARAMS + WHITE_SPACE_SEPARATOR + "*";
    static final Pattern SUB_TOKEN_PATTERN = Pattern.compile("\\s*(?<" + TOKEN + ">((\"[^\"]*\")|([^\\s\"]+))*)\\s*" );
    static final String KEY_NAME = "name";
    static final String KEY_PATH = "path";
    static final String KEY_EXPR = "expr";
    static final String DECL_BINDING = "binding";
    static final String DECL_BINDING_CONTENT = "declcontent";
    static final Pattern DECL_BINDING_PATTERN = Pattern.compile(DECL_BINDING + WHITE_SPACE_SEPARATOR + "*"
            + "(?<" + DECL_BINDING + ">[\\w_\\-\\d]+)"
            + WHITE_SPACE_SEPARATOR + "*=" + WHITE_SPACE_SEPARATOR + "*" +
            "(?<" +  DECL_BINDING_CONTENT + ">.*)");

    private static final String HELP_START =
        "\n a <pipe token> is <pipe> <expr|conf>? (<options>)?" +
        "\n <options> are (@ <option>)* form with <option> being either" +
        "\n\t'name pipeName' (used in bindings), " +
        "\n\t'expr pipeExpression' (when not directly as <args>)" +
        "\n\t'path pipePath' (when not directly as <args>)" +
        "\n\t'bindings key=value ...' (for setting pipe bindings) " +
        "\n\t'with key=value ...' (for setting pipe specific properties)" +
        "\n\t'outputs key=value ...' (for setting outputs)" +
        "\n and <pipe> is one of the following :\n";

    Map<String, Method> methodMap;
    Map<String, PipeExecutor> executorMap;

    String help;

    @Reference
    Plumber plumber;

    boolean enabled = false;

    @Activate
    @Modified
    public void activate(Configuration configuration){
        enabled = configuration.enabled();
        methodMap = null;
        executorMap = null;
        help = null;
    }

    @ObjectClassDefinition(name="Apache Sling Pipes : Command Executor Configuration")
    public @interface Configuration {
        @AttributeDefinition(description="Enable command executor to be executed from servlet, if not, sends 503")
        boolean enabled() default true;
    }

    boolean isBlankLine(String line) {
        return StringUtils.isBlank(line) || line.startsWith(COMMENT_PREFIX);
    }

    boolean isJsonBinding(String line) {
        return line.indexOf(DECL_BINDING) >= 0;
    }

    private void handleInputEnd(StringBuilder builder, String currentBinding, List<String> cmds, Map<String, Object> bindings) {
        if (StringUtils.isBlank(currentBinding) && builder.length() > 0) {
            cmds.add(builder.toString().trim());
        } else if (StringUtils.isNoneBlank(currentBinding, builder)) {
            bindings.put(currentBinding, builder.toString().trim());
        }
    }

    List<String> getCommandList(SlingHttpServletRequest request, Map<String, Object> bindings) throws IOException {
        List<String> cmds = new ArrayList<>();
        if (request.getParameterMap().containsKey(REQ_PARAM_CMD)) {
            cmds.add(request.getParameter(REQ_PARAM_CMD));
        } else {
            RequestParameter paramFile = request.getRequestParameter(REQ_PARAM_FILE);
            if (paramFile != null) {
                InputStream is = paramFile.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                String line;
                String currentBinding = null;
                StringBuilder readerBuilder = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    if (isBlankLine(line)) {
                        if (readerBuilder.length() > 0) {
                            handleInputEnd(readerBuilder, currentBinding, cmds, bindings);
                            readerBuilder = new StringBuilder();
                            currentBinding = null;
                        }
                    }  else if (isJsonBinding(line)) {
                        Matcher matcher = DECL_BINDING_PATTERN.matcher(line);
                        if (matcher.find()) {
                            currentBinding = matcher.group(DECL_BINDING);
                            readerBuilder.append(matcher.group(DECL_BINDING_CONTENT).trim());
                        }
                    } else {
                        //depending on what we are appending, we keep lines return or not
                        readerBuilder.append((StringUtils.isBlank(currentBinding) ? LINE_SEPARATOR : "\n") + line.trim());
                    }
                }
                handleInputEnd(readerBuilder, currentBinding, cmds, bindings);
            }
        }
        return cmds;
    }

    protected void executeCommands(@NotNull SlingHttpServletRequest request, @NotNull SlingHttpServletResponse response) throws IOException {
        PrintWriter writer = response.getWriter();
        String currentCommand = null;
        try {
            ResourceResolver resolver = request.getResourceResolver();
            Map<String, Object> bindings = plumber.getBindingsFromRequest(request, true);
            List<String> cmds = getCommandList(request, bindings);
            if (cmds.isEmpty()) {
                writer.println("No command to execute!");
            }
            short idxLine = 0;

            OutputWriter pipeWriter = getWriter(request, response);
            if (pipeWriter == null) {
                pipeWriter = new NopWriter();
            }
            pipeWriter.disableAutoClose();
            pipeWriter.init(request, response);
            for (String command : cmds) {
                if (StringUtils.isNotBlank(command)) {
                    currentCommand = command;
                    PipeBuilder pipeBuilder = parse(resolver, command);
                    Pipe pipe = pipeBuilder.build();
                    bindings.put(CMD_LINE_PREFIX + idxLine++, pipe.getResource().getPath());
                    ModifiableValueMap root = pipe.getResource().adaptTo(ModifiableValueMap.class);
                    if (root != null) {
                        root.put(PN_DESCRIPTION, command);
                    }
                    plumber.execute(resolver, pipe, bindings, pipeWriter, true);
                }
            }
            pipeWriter.ends();
        }  catch (IllegalAccessException | InvocationTargetException e) {
            writer.println("Error executing " + currentCommand);
            e.printStackTrace(writer);
            response.setStatus(SC_INTERNAL_SERVER_ERROR);
            response.sendError(SC_INTERNAL_SERVER_ERROR);
            writer.println(help());
        }
        catch (IllegalArgumentException | JsonException e) {
            writer.println("Error executing " +  currentCommand);
            e.printStackTrace(writer);
            response.setStatus(SC_NOT_ACCEPTABLE);
            response.sendError(SC_NOT_ACCEPTABLE);
            writer.println(help());
        }
    }

    @Override
    protected void doPost(@NotNull SlingHttpServletRequest request, @NotNull SlingHttpServletResponse response) throws IOException {
        if (enabled) {
            PrintWriter writer = response.getWriter();
            try {
                if (request.getParameter(REQ_PARAM_HELP) != null) {
                    writer.println(help());
                } else {
                    executeCommands(request, response);
                }
                writer.println("");
                response.setStatus(SC_OK);
            }
            catch (AccessControlException e) {
                response.setStatus(SC_FORBIDDEN);
                response.sendError(SC_FORBIDDEN);
            }    
        } else {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "http service has been disabled");
        }
    }

    @Override
    public ExecutionResult execute(ResourceResolver resolver, String path, String... optionTokens) {
        Resource resource = resolver.getResource(path);
        if (resource == null){
            throw new IllegalArgumentException(String.format("%s resource does not exist", path));
        }
        Options options = getOptions(optionTokens);
        Map<String, Object> bMap = null;
        if (options.with != null) {
            bMap = new HashMap<>();
            writeToMap(bMap, true, options.with);
        }
        OutputWriter writer = new NopWriter();
        if (options.outputs != null){
            writer = new JsonWriter();
            Map<String, Object> outputs = new HashMap<>();
            CommandUtil.writeToMap(outputs, true, options.outputs);
            writer.setCustomOutputs(outputs);
        }
        writer.starts();
        return plumber.execute(resolver, path, bMap, writer, true);
    }

    @Override
    public PipeBuilder parse(ResourceResolver resolver, String...cmds) throws InvocationTargetException, IllegalAccessException {
        PipeBuilder builder = plumber.newPipe(resolver);
        for (Token token : parseTokens(cmds)){
            Method method = getMethodMap().get(token.pipeKey);
            if (method == null){
                throw new IllegalArgumentException(token.pipeKey + " is not a valid pipe");
            }
            if (isExpressionExpected(method)){
                method.invoke(builder, token.args.get(0));
            } else if (isConfExpected(method)){
                method.invoke(builder, (Object)keyValuesToArray(token.args));
            } else if (isWithoutExpectedParameter(method)){
                method.invoke(builder);
            }

            if (token.options != null){
                token.options.writeToBuilder(builder);
            }
        }
        return builder;

    }

    /**
     * @param tokens array of tokens
     * @return options from array
     */
    protected Options getOptions(String[] tokens){
        return getOptions(Arrays.asList(tokens));
    }

    /**
     * @param tokens list of tokens
     * @return options from token list
     */
    protected Options getOptions(List<String> tokens){
        return new Options(tokens);
    }

    /**
     * Options for a pipe execution
     */
    protected class Options {
        String name;
        String path;
        String expr;
        String[] with;
        String[] bindings;
        String[] outputs;

        @Override
        public String toString() {
            return "Options{" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                ", expr='" + expr + '\'' +
                ", with=" + Arrays.toString(with) +
                ", bindings=" + Arrays.toString(bindings) +
                ", outputs=" + Arrays.toString(outputs) +
                '}';
        }

        void setOutputs(List<String> values) {
            this.outputs = keyValuesToArray(values);
        }

        /**
         * Constructor
         * @param options string list from where options will be built
         */
        protected Options(List<String> options){
            Map<String, Object> optionMap = new HashMap<>();
            for (String optionToken : options) {
                String currentKey = null;
                List<String> currentList = new ArrayList<>();
                for (String subToken : getSpaceSeparatedTokens(optionToken)) {
                    if (currentKey == null) {
                        currentKey = subToken;
                    } else {
                        currentList.add(subToken);
                    }
                }
                finishOption(currentKey, currentList, optionMap);
            }
            for (Map.Entry<String, Object> entry : optionMap.entrySet()){
                switch (entry.getKey()) {
                    case Pipe.PN_NAME :
                        this.name = (String)entry.getValue();
                        break;
                    case Pipe.PN_PATH :
                        this.path = (String)entry.getValue();
                        break;
                    case Pipe.PN_EXPR :
                        this.expr = (String)entry.getValue();
                        break;
                    case "with" :
                        this.with = keyValuesToArray((List<String>)entry.getValue());
                        break;
                    case "bindings" :
                        this.bindings = keyValuesToArray((List<String>) entry.getValue());
                        break;
                    case "outputs" :
                        setOutputs((List<String>)entry.getValue());
                        break;
                    default:
                        throw new IllegalArgumentException(String.format("%s is an unknown option", entry.getKey()));
                }
            }
        }

        /**
         * wrap up current option
         * @param currentKey option key
         * @param currentList list being processed
         * @param optionMap option map
         */
        protected void finishOption(String currentKey, List<String> currentList, Map<String, Object> optionMap){
            if (currentList != null){
                if (currentKey.equals(KEY_NAME) || currentKey.equals(KEY_EXPR) || currentKey.equals(KEY_PATH)) {
                    optionMap.put(currentKey, currentList.get(0));
                } else {
                    optionMap.put(currentKey, currentList);
                }
            }
        }

        /**
         * write options to current builder
         * @param builder current builder
         * @throws IllegalAccessException
         */
        void writeToBuilder(PipeBuilder builder) throws IllegalAccessException {
            if (StringUtils.isNotBlank(name)){
                builder.name(name);
            }
            if (StringUtils.isNotBlank(path)){
                builder.path(path);
            }
            if (StringUtils.isNotBlank(expr)){
                builder.expr(expr);
            }
            if (with != null){
                builder.with(with);
            }
            if (bindings != null){
                builder.bindings(bindings);
            }
            if (outputs != null) {
                builder.outputs(outputs);
            }
        }
    }

    /**
     * Pipe token, used to hold information of a "sub pipe" configuration
     */
    protected class Token {
        String pipeKey;
        List<String> args;
        CommandExecutorImpl.Options options;

        @Override
        public String toString() {
            return "Token{" +
                "pipeKey='" + pipeKey + '\'' +
                ", args=" + args +
                ", options=" + options +
                '}';
        }
    }

    List<String> getSpaceSeparatedTokens(String token) {
        List<String> subTokens = new ArrayList<>();
        Matcher matcher = SUB_TOKEN_PATTERN.matcher(token);
        while (matcher.find()){
            String subToken = matcher.group(TOKEN);
            if (StringUtils.isNotBlank(subToken)) {
                subTokens.add(CommandUtil.trimQuotes(subToken));
            }
        }
        return subTokens;
    }

    /**
     * @param commands full list of command tokens
     * @return Token list corresponding to the string ones
     */
    protected List<CommandExecutorImpl.Token> parseTokens(String... commands) {
        List<CommandExecutorImpl.Token> returnValue = new ArrayList<>();
        String cat = String.join(EMPTY, commands);
        for (String token : cat.split(PIPE_SEPARATOR)){
            CommandExecutorImpl.Token currentToken = new CommandExecutorImpl.Token();
            String[] options = token.split(PARAMS_SEPARATOR);
            if (options.length > 1) {
                currentToken.options = getOptions(Arrays.copyOfRange(options, 1, options.length));
            }
            List<String> subTokens = getSpaceSeparatedTokens(options[0]);
            if (! subTokens.isEmpty()) {
                currentToken.pipeKey = subTokens.get(0);
                if (subTokens.size() > 1) {
                    currentToken.args = subTokens.subList(1, subTokens.size());
                    if (JSON_EXPR_KEYS.contains(currentToken.pipeKey) &&
                            JSON_START.indexOf(currentToken.args.get(0).getBytes(StandardCharsets.UTF_8)[0]) > 0) {
                        //in that case we want to concatenate all subsequent 'args' as it is a JSON expression
                        currentToken.args = Collections.singletonList(String.join(EMPTY, currentToken.args));
                    }
                }
            }
            log.trace("generated following token {}", currentToken);
            returnValue.add(currentToken);
        }
        return returnValue;
    }

    /**
     * builds utility maps
     */
    protected void computeMaps(){
        executorMap = new HashMap<>();
        methodMap = new HashMap<>();
        for (Method method : PipeBuilderImpl.class.getDeclaredMethods()) {
            PipeExecutor executor = method.getAnnotation(PipeExecutor.class);
            if (executor != null) {
                methodMap.put(executor.command(), method);
                executorMap.put(executor.command(), executor);
            }
        }
    }

    /**
     * @return map of command to PB api method
     */
    protected Map<String, Method> getMethodMap() {
        if (methodMap == null) {
            computeMaps();
        }
        return methodMap;
    }

    /**
     * @return map of command to Annotation information around the PB api
     */
    protected Map<String, PipeExecutor> getExecutorMap() {
        if (executorMap == null) {
            computeMaps();
        }
        return executorMap;
    }

    /**
     * @param method corresponding PB api
     * @return true if the api does expect an expression (meaning a string)
     */
    protected boolean isExpressionExpected(Method method) {
        return method.getParameterCount() == 1 && method.getParameterTypes()[0].equals(String.class);
    }

    /**
     * @param method corresponding PB api
     * @return true if the api does expect a configuration (meaning a list of key value pairs)
     */
    protected boolean isConfExpected(Method method) {
        return method.getParameterCount() == 1 && method.getParameterTypes()[0].equals(Object[].class);
    }

    /**
     * @param method corresponding PB api
     * @return true if the api does not expect parameters
     */
    protected boolean isWithoutExpectedParameter(Method method){
        return method.getParameterCount() == 0;
    }

    /**
     * help command handler
     */
    public String help(){
        if (StringUtils.isBlank(help)) {
            StringBuilder builder = new StringBuilder();
            builder.append(HELP_START);
            for (Map.Entry<String, PipeExecutor> entry : getExecutorMap().entrySet()) {
                builder.append(String.format("\t%s\t\t:\t%s%n", entry.getKey(), entry.getValue().description()));
            }
            help = builder.toString();
        }
        return help;
    }

}
