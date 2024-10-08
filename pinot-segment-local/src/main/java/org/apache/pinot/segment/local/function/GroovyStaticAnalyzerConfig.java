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
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.utils.JsonUtils;


public class GroovyStaticAnalyzerConfig {
  final boolean _enabled;
  final private List<String> _allowedReceivers;
  final private List<String> _allowedImports;
  final private List<String> _allowedStaticImports;
  final private List<String> _disallowedMethodNames;

  public GroovyStaticAnalyzerConfig(
      @JsonProperty("enabled")
      boolean enabled,
      @JsonProperty("allowedReceivers")
      List<String> allowedReceivers,
      @JsonProperty("allowedImports")
      List<String> allowedImports,
      @JsonProperty("allowedStaticImports")
      List<String> allowedStaticImports,
      @JsonProperty("disallowedMethodNames")
      List<String> disallowedMethodNames) {
    _enabled = enabled;
    _allowedImports = allowedImports;
    _allowedReceivers = allowedReceivers;
    _allowedStaticImports = allowedStaticImports;
    _disallowedMethodNames = disallowedMethodNames;
  }

  @JsonProperty("enabled")
  public boolean isEnabled() {
    return _enabled;
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

  public ZNRecord toZNRecord() throws JsonProcessingException {
    ZNRecord record = new ZNRecord("groovySecurityConfiguration");
    record.setSimpleField("staticAnalyzerConfig", JsonUtils.objectToString(this));
    return record;
  }

  public static GroovyStaticAnalyzerConfig fromZNRecord(ZNRecord zr) throws JsonProcessingException {
    Preconditions.checkArgument(zr.getId().equals("groovySecurityConfiguration"),
        "Expected ZNRecord with ID \"groovySecurityConfiguration\" but got {}", zr.getId());

    final String configJson = zr.getSimpleField("staticAnalyzerConfig");
    Preconditions.checkState(configJson != null && !configJson.isEmpty(),
        "Empty JSON String");

    return JsonUtils.stringToObject(configJson, GroovyStaticAnalyzerConfig.class);
  }

  public static List<Class> getDefaultAllowedTypes() {
    return List.of(
        Integer.class,
        Float.class,
        Long.class,
        Double.class,
        Integer.TYPE,
        Long.TYPE,
        Float.TYPE,
        Double.TYPE,
        String.class,
        Object.class,
        Byte.class,
        Byte.TYPE,
        BigDecimal.class,
        BigInteger.class
    );
  }

  public static List<String> getDefaultAllowedReceivers() {
    return List.of(String.class.getName(), Math.class.getName());
  }

  public static List<String> getDefaultAllowedImports() {
    return List.of(Math.class.getName());
  }

  public static GroovyStaticAnalyzerConfig createDefault(boolean enabled) {
    return new GroovyStaticAnalyzerConfig(
        enabled,
        GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers(),
        GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
        GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
        List.of("execute", "invoke")
    );
  }
}
