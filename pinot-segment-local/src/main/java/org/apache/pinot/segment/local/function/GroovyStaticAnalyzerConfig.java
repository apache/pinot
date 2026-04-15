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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;


/**
 * Deprecated forwarding wrapper for the legacy Groovy static analyzer config type name.
 *
 * <p>This value object is immutable and thread-safe.
 *
 * <p>TODO: Delete this shim after Pinot 1.6.0 is released.
 *
 * @deprecated Use {@link org.apache.pinot.common.evaluator.GroovyStaticAnalyzerConfig} instead.
 */
@Deprecated
public class GroovyStaticAnalyzerConfig extends org.apache.pinot.common.evaluator.GroovyStaticAnalyzerConfig {
  public GroovyStaticAnalyzerConfig(
      @JsonProperty("allowedReceivers")
      List<String> allowedReceivers,
      @JsonProperty("allowedImports")
      List<String> allowedImports,
      @JsonProperty("allowedStaticImports")
      List<String> allowedStaticImports,
      @JsonProperty("disallowedMethodNames")
      List<String> disallowedMethodNames,
      @JsonProperty("methodDefinitionAllowed")
      boolean methodDefinitionAllowed) {
    super(allowedReceivers, allowedImports, allowedStaticImports, disallowedMethodNames, methodDefinitionAllowed);
  }

  public static GroovyStaticAnalyzerConfig fromJson(String configJson)
      throws JsonProcessingException {
    org.apache.pinot.common.evaluator.GroovyStaticAnalyzerConfig config =
        org.apache.pinot.common.evaluator.GroovyStaticAnalyzerConfig.fromJson(configJson);
    return copy(config);
  }

  public static List<Class> getDefaultAllowedTypes() {
    return org.apache.pinot.common.evaluator.GroovyStaticAnalyzerConfig.getDefaultAllowedTypes();
  }

  public static List<String> getDefaultAllowedReceivers() {
    return org.apache.pinot.common.evaluator.GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers();
  }

  public static List<String> getDefaultAllowedImports() {
    return org.apache.pinot.common.evaluator.GroovyStaticAnalyzerConfig.getDefaultAllowedImports();
  }

  public static GroovyStaticAnalyzerConfig createDefault() {
    return copy(org.apache.pinot.common.evaluator.GroovyStaticAnalyzerConfig.createDefault());
  }

  private static GroovyStaticAnalyzerConfig copy(
      org.apache.pinot.common.evaluator.GroovyStaticAnalyzerConfig config) {
    return new GroovyStaticAnalyzerConfig(config.getAllowedReceivers(), config.getAllowedImports(),
        config.getAllowedStaticImports(), config.getDisallowedMethodNames(), config.isMethodDefinitionAllowed());
  }
}
