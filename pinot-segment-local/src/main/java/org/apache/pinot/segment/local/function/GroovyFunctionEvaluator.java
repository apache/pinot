/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.codehaus.groovy.control.customizers.SecureASTCustomizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link FunctionEvaluator} for evaluating transform function expressions of a Schema field spec written in Groovy.
 * GroovyShell is used to execute expressions.
 *
 * The transform expression must follow the convention Groovy({expression}, arguments1, argument2...)
 * For example:
 * "dimensionFieldSpecs": [
 *     {
 *       "name": "fullName",
 *       "dataType": "STRING",
 *       "transformFunction": "Groovy({firstName+' '+lastName}, firstName, lastName)"
 *     }
 *  ]
 */
public class GroovyFunctionEvaluator implements FunctionEvaluator {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroovyFunctionEvaluator.class);

  private static final String GROOVY_EXPRESSION_PREFIX = "Groovy";
  private static final String GROOVY_FUNCTION_REGEX = "Groovy\\(\\{(?<script>.+)}(,(?<arguments>.+))?\\)";
  private static final Pattern GROOVY_FUNCTION_PATTERN =
      Pattern.compile(GROOVY_FUNCTION_REGEX, Pattern.CASE_INSENSITIVE);
  private static final String ARGUMENTS_GROUP_NAME = "arguments";
  private static final String SCRIPT_GROUP_NAME = "script";
  private static final String ARGUMENTS_SEPARATOR = ",";
  private static GroovyStaticAnalyzerConfig _groovyStaticAnalyzerConfig;

  private final List<String> _arguments;
  private final int _numArguments;
  private final Binding _binding;
  private final Script _script;
  private final String _expression;
  private static CompilerConfiguration _compilerConfiguration = new CompilerConfiguration();

  public GroovyFunctionEvaluator(String closure) {
    _expression = closure;
    Matcher matcher = GROOVY_FUNCTION_PATTERN.matcher(closure);
    Preconditions.checkState(matcher.matches(), "Invalid transform expression: %s", closure);
    String arguments = matcher.group(ARGUMENTS_GROUP_NAME);
    if (arguments != null) {
      _arguments = Splitter.on(ARGUMENTS_SEPARATOR).trimResults().splitToList(arguments);
    } else {
      _arguments = Collections.emptyList();
    }
    _numArguments = _arguments.size();
    _binding = new Binding();
    String scriptText = matcher.group(SCRIPT_GROUP_NAME);
    try {
      _script = createSafeShell(_binding).parse(scriptText);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to compile groovy script in transform function with exception:" + e.getMessage(), e);
    }
  }

  public static String getGroovyExpressionPrefix() {
    return GROOVY_EXPRESSION_PREFIX;
  }

  /**
   * This method is used to parse the Groovy script and check if the script is valid.
   * @param script Groovy script to be parsed.
   */
  public static void parseGroovyScript(String script) {
    Matcher matcher = GROOVY_FUNCTION_PATTERN.matcher(script);
    Preconditions.checkState(matcher.matches(), "Invalid transform expression: %s", script);
    String scriptText = matcher.group(SCRIPT_GROUP_NAME);
    new GroovyShell(new Binding(), _compilerConfiguration).parse(scriptText);
  }

  /**
   * This will create a Groovy Shell that is configured with static syntax analysis. This static syntax analysis
   * will that any script which is run is restricted to a specific list of allowed operations, thus making it harder
   * to execute malicious code.
   *
   * @param binding Binding instance to be used by Groovy Shell.
   * @return GroovyShell instance with static syntax analysis.
   */
  private GroovyShell createSafeShell(Binding binding) {
    return new GroovyShell(binding, _compilerConfiguration);
  }

  @Override
  public List<String> getArguments() {
    return _arguments;
  }

  @Override
  public Object evaluate(GenericRow genericRow) {
    boolean hasNullArgument = false;
    for (String argument : _arguments) {
      Object value = genericRow.getValue(argument);
      if (value == null) {
        hasNullArgument = true;
      }
      _binding.setVariable(argument, value);
    }
    try {
      return _script.run();
    } catch (Exception e) {
      if (hasNullArgument) {
        return null;
      } else {
        throw e;
      }
    }
  }

  @Override
  public Object evaluate(Object[] values) {
    for (int i = 0; i < _numArguments; i++) {
      _binding.setVariable(_arguments.get(i), values[i]);
    }
    return _script.run();
  }

  @Override
  public String toString() {
    return _expression;
  }

  public static void configureGroovySecurity(String groovyASTConfig)
      throws Exception {
    try {
      if (groovyASTConfig != null) {
        setGroovyStaticAnalyzerConfig(GroovyStaticAnalyzerConfig.fromJson(groovyASTConfig));
      } else {
        LOGGER.info("No Groovy Security Configuration found, Groovy static analysis is disabled");
      }
    } catch (Exception ex) {
      throw new Exception("Failed to configure Groovy Security", ex);
    }
  }

  /**
   * Initialize or update the configuration for the Groovy Static Analyzer.
   * Update compiler configuration to include the new configuration.
   * @param groovyStaticAnalyzerConfig GroovyStaticAnalyzerConfig instance to be used for static syntax analysis.
   */
  public static void setGroovyStaticAnalyzerConfig(GroovyStaticAnalyzerConfig groovyStaticAnalyzerConfig)
      throws JsonProcessingException {
    synchronized (GroovyFunctionEvaluator.class) {
      _groovyStaticAnalyzerConfig = groovyStaticAnalyzerConfig;
      if (groovyStaticAnalyzerConfig != null) {
        _compilerConfiguration = createSecureGroovyConfig();
        LOGGER.info("Setting Groovy Static Analyzer Config: {}", groovyStaticAnalyzerConfig.toJson());
      } else {
        _compilerConfiguration = new CompilerConfiguration();
        LOGGER.info("Disabling Groovy Static Analysis");
      }
    }
  }

  private static CompilerConfiguration createSecureGroovyConfig() {
    GroovyStaticAnalyzerConfig groovyConfig = _groovyStaticAnalyzerConfig;
    ImportCustomizer imports = new ImportCustomizer().addStaticStars("java.lang.Math");
    SecureASTCustomizer secure = new SecureASTCustomizer();

    secure.addExpressionCheckers(expression -> {
      if (expression instanceof MethodCallExpression) {
        MethodCallExpression method = (MethodCallExpression) expression;
        return !groovyConfig.getDisallowedMethodNames().contains(method.getMethodAsString());
      } else {
        return true;
      }
    });

    secure.setConstantTypesClassesWhiteList(GroovyStaticAnalyzerConfig.getDefaultAllowedTypes());
    secure.setImportsWhitelist(groovyConfig.getAllowedImports());
    secure.setStaticImportsWhitelist(groovyConfig.getAllowedImports());
    secure.setReceiversWhiteList(groovyConfig.getAllowedReceivers());

    // Block all * imports
    secure.setStaticStarImportsWhitelist(groovyConfig.getAllowedImports());
    secure.setStarImportsWhitelist(groovyConfig.getAllowedImports());

    // Allow all expression and token types
    secure.setExpressionsBlacklist(List.of());
    secure.setTokensBlacklist(List.of());

    secure.setMethodDefinitionAllowed(groovyConfig.isMethodDefinitionAllowed());
    secure.setIndirectImportCheckEnabled(true);
    secure.setClosuresAllowed(true);
    secure.setPackageAllowed(false);

    CompilerConfiguration compilerConfiguration = new CompilerConfiguration();
    return compilerConfiguration.addCompilationCustomizers(imports, secure);
  }
}
