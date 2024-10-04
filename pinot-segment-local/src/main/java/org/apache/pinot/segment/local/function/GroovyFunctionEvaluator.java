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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jnr.ffi.annotations.In;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.codehaus.groovy.ast.ASTNode;
import org.codehaus.groovy.ast.builder.AstBuilder;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.codehaus.groovy.control.customizers.SecureASTCustomizer;
import org.codehaus.groovy.syntax.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.local.function.GroovyStaticAnalyzerConfig.getDefaultAllowedImports;
import static org.apache.pinot.segment.local.function.GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers;
import static org.apache.pinot.segment.local.function.GroovyStaticAnalyzerConfig.getDefaultAllowedTypes;
import static org.codehaus.groovy.syntax.Types.*;


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
  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigUtils.class);

  private static final String GROOVY_EXPRESSION_PREFIX = "Groovy";
  private static final String GROOVY_FUNCTION_REGEX = "Groovy\\(\\{(?<script>.+)}(,(?<arguments>.+))?\\)";
  private static final Pattern GROOVY_FUNCTION_PATTERN =
      Pattern.compile(GROOVY_FUNCTION_REGEX, Pattern.CASE_INSENSITIVE);
  private static final String ARGUMENTS_GROUP_NAME = "arguments";
  private static final String SCRIPT_GROUP_NAME = "script";
  private static final String ARGUMENTS_SEPARATOR = ",";

  private final List<String> _arguments;
  private final int _numArguments;
  private final Binding _binding;
  private final Script _script;
  private final String _expression;
  private final boolean _isInvalid;

  public GroovyFunctionEvaluator(String closure) {
    this(closure, null);
  }

  public GroovyFunctionEvaluator(String closure, GroovyStaticAnalyzerConfig groovyConfig) {
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
    final String scriptText = matcher.group(SCRIPT_GROUP_NAME);

    final GroovyStaticAnalyzerConfig groovyStaticAnalyzerConfig = groovyConfig != null
        ? groovyConfig
        : new GroovyStaticAnalyzerConfig(
            false,
            getDefaultAllowedReceivers(),
            getDefaultAllowedImports(),
            getDefaultAllowedImports(),
            List.of("invoke", "execute"));
    _isInvalid = methodSanitizer(groovyStaticAnalyzerConfig, scriptText);
    _script = createSafeShell(_binding, groovyStaticAnalyzerConfig).parse(scriptText);
  }

  public static String getGroovyExpressionPrefix() {
    return GROOVY_EXPRESSION_PREFIX;
  }

  private boolean methodSanitizer(GroovyStaticAnalyzerConfig config, String script) {
    if (config.isEnabled()) {
      AstBuilder astBuilder = new AstBuilder();
      List<ASTNode> ast = astBuilder.buildFromString(script);
      GroovyMethodSanitizer visitor = new GroovyMethodSanitizer(config.getDisallowedMethodNames());
      ast.forEach(node -> node.visit(visitor));

      return visitor.isInvalid();
    }

    return false;
  }

  /**
   * This will create a Groovy Shell that is configured with static syntax analysis. This static syntax analysis
   * will that any script which is run is restricted to a specific list of allowed operations, thus making it harder
   * to execute malicious code.
   *
   * @param binding Binding instance to be used by Groovy Shell.
   * @return
   */
  private GroovyShell createSafeShell(Binding binding, GroovyStaticAnalyzerConfig groovyConfig) {
    CompilerConfiguration config = new CompilerConfiguration();

    if (groovyConfig.isEnabled()) {
      final ImportCustomizer imports = new ImportCustomizer().addStaticStars("java.lang.Math");
      final SecureASTCustomizer secure = new SecureASTCustomizer();

      secure.setConstantTypesClassesWhiteList(getDefaultAllowedTypes());
      secure.setImportsWhitelist(getDefaultAllowedImports());
      secure.setStaticImportsWhitelist(getDefaultAllowedImports());
      secure.setReceiversWhiteList(getDefaultAllowedReceivers());

      // Block all * imports
      secure.setStaticStarImportsWhitelist(List.of());
      secure.setStarImportsWhitelist(List.of());

      // Allow all expression and token types
      secure.setExpressionsBlacklist(List.of());
      secure.setTokensBlacklist(List.of());

      secure.setIndirectImportCheckEnabled(true);
      secure.setClosuresAllowed(false);
      secure.setPackageAllowed(false);

      config.addCompilationCustomizers(imports, secure);
    }

    return new GroovyShell(binding, config);
  }

  @Override
  public List<String> getArguments() {
    return _arguments;
  }

  @Override
  public Object evaluate(GenericRow genericRow) {
    if (_isInvalid) {
      LOGGER.error("Attempted to execute an illegal Groovy script");
      throw new RuntimeException("Groovy script execution failure");
    }

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
    if (_isInvalid) {
      LOGGER.error("Attempted to execute an illegal Groovy script");
      throw new RuntimeException("Groovy script execution failure");
    }

    for (int i = 0; i < _numArguments; i++) {
      _binding.setVariable(_arguments.get(i), values[i]);
    }
    return _script.run();
  }

  @Override
  public String toString() {
    return _expression;
  }
}
