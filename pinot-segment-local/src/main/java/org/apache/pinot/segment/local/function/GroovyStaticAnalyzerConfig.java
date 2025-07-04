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
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.JsonUtils;


public class GroovyStaticAnalyzerConfig {
  private final List<String> _allowedReceivers;
  private final List<String> _allowedImports;
  private final List<String> _allowedStaticImports;
  private final List<String> _disallowedMethodNames;
  private final boolean _methodDefinitionAllowed;

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
    _allowedImports = allowedImports;
    _allowedReceivers = allowedReceivers;
    _allowedStaticImports = allowedStaticImports;
    _disallowedMethodNames = disallowedMethodNames;
    _methodDefinitionAllowed = methodDefinitionAllowed;
  }

  @JsonProperty("allowedReceivers")
  public List<String> getAllowedReceivers() {
    return _allowedReceivers;
  }

  @JsonProperty("allowedImports")
  public List<String> getAllowedImports() {
    return _allowedImports;
  }

  @JsonProperty("allowedStaticImports")
  public List<String> getAllowedStaticImports() {
    return _allowedStaticImports;
  }

  @JsonProperty("disallowedMethodNames")
  public List<String> getDisallowedMethodNames() {
    return _disallowedMethodNames;
  }

  @JsonProperty("methodDefinitionAllowed")
  public boolean isMethodDefinitionAllowed() {
    return _methodDefinitionAllowed;
  }

  public String toJson() throws JsonProcessingException {
    return JsonUtils.objectToString(this);
  }

  public static GroovyStaticAnalyzerConfig fromJson(String configJson) throws JsonProcessingException {
    Preconditions.checkState(StringUtils.isNotBlank(configJson), "Empty groovySecurityConfiguration JSON string");

    return JsonUtils.stringToObject(configJson, GroovyStaticAnalyzerConfig.class);
  }

  public static List<Class> getDefaultAllowedTypes() {
    return List.of(
        Integer.class,
        Float.class,
        Long.class,
        Double.class,
        String.class,
        Object.class,
        Byte.class,
        BigDecimal.class,
        BigInteger.class,
        Integer.TYPE,
        Long.TYPE,
        Float.TYPE,
        Double.TYPE,
        Byte.TYPE
    );
  }

  public static List<String> getDefaultAllowedReceivers() {
    return List.of(
        String.class.getName(),
        Math.class.getName(),
        java.util.List.class.getName(),
        Object.class.getName(),
        java.util.Map.class.getName()
    );
  }

  public static List<String> getDefaultAllowedImports() {
    return List.of(
        Math.class.getName(),
        java.util.List.class.getName(),
        String.class.getName(),
        java.util.Map.class.getName()
    );
  }

  public static GroovyStaticAnalyzerConfig createDefault() {
    return new GroovyStaticAnalyzerConfig(
        GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers(),
        GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
        GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
        List.of("execute", "invoke"),
        false
    );
  }
}
