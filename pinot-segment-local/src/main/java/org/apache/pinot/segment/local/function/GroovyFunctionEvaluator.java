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


/**
 * Deprecated forwarding wrapper for the legacy Groovy evaluator type name.
 *
 * <p>Instances inherit the thread-safety characteristics of
 * {@link org.apache.pinot.common.evaluator.GroovyFunctionEvaluator}.
 *
 * <p>TODO: Delete this shim after Pinot 1.6.0 is released.
 *
 * @deprecated Use {@link org.apache.pinot.common.evaluator.GroovyFunctionEvaluator} instead.
 */
@Deprecated
public class GroovyFunctionEvaluator extends org.apache.pinot.common.evaluator.GroovyFunctionEvaluator
    implements FunctionEvaluator {
  public GroovyFunctionEvaluator(String closure) {
    super(closure);
  }

  public static String getGroovyExpressionPrefix() {
    return org.apache.pinot.common.evaluator.GroovyFunctionEvaluator.getGroovyExpressionPrefix();
  }

  public static void parseGroovyScript(String script) {
    org.apache.pinot.common.evaluator.GroovyFunctionEvaluator.parseGroovyScript(script);
  }

  public static void configureGroovySecurity(String groovyASTConfig)
      throws Exception {
    org.apache.pinot.common.evaluator.GroovyFunctionEvaluator.configureGroovySecurity(groovyASTConfig);
  }

  public static void setGroovyStaticAnalyzerConfig(GroovyStaticAnalyzerConfig groovyStaticAnalyzerConfig)
      throws JsonProcessingException {
    org.apache.pinot.common.evaluator.GroovyFunctionEvaluator.setGroovyStaticAnalyzerConfig(groovyStaticAnalyzerConfig);
  }
}
