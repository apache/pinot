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
package org.apache.pinot.common.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.utils.CommonConstants;


/// Factory class to create an [FunctionEvaluator] for the field spec based on the
/// [FieldSpec#getTransformFunction()]
public class FunctionEvaluatorFactory {
  private static final String MAP_KEY_COLUMN_SUFFIX = "__KEYS";
  private static final String MAP_VALUE_COLUMN_SUFFIX = "__VALUES";
  private static final String DISABLED_GROOVY_MESSAGE = String.format(
      "Groovy ingestion functions are disabled. Set '%s=false' to enable them",
      CommonConstants.Groovy.DISABLE_INGESTION_GROOVY);

  // Standalone segment-generation jobs do not pass through service startup config, so allow only an explicit system
  // property value of false to opt in. Missing or invalid values remain fail-closed.
  private static volatile boolean _ingestionGroovyDisabled = readIngestionGroovyDisabledFromSystemProperty();

  private FunctionEvaluatorFactory() {
  }

  /// Creates the [FunctionEvaluator] for the given field spec
  ///
  /// 1. If transform expression is defined, use it to create the appropriate [FunctionEvaluator]
  /// 2. For TIME column, if conversion is needed, [TimeSpecFunctionEvaluator] for backward compatible handling
  /// of time spec. This
  /// is needed until we migrate to [org.apache.pinot.spi.data.DateTimeFieldSpec]
  /// 3. For columns ending with \_\_KEYS or \_\_VALUES (used for interpreting Map columns in Avro), create a
  /// backward-compatible map evaluator
  /// 4. Return null, if none of the above
  @Nullable
  public static FunctionEvaluator getExpressionEvaluator(FieldSpec fieldSpec) {
    return getExpressionEvaluator(fieldSpec, _ingestionGroovyDisabled);
  }

  /// Creates an ingestion evaluator using an explicit Groovy policy.
  @Nullable
  public static FunctionEvaluator getExpressionEvaluator(FieldSpec fieldSpec, boolean ingestionGroovyDisabled) {
    FunctionEvaluator functionEvaluator = null;

    String columnName = fieldSpec.getName();
    // TODO: once we have published a release w/ IngestionConfig#TransformConfigs, stop reading transform function
    //  from schema in next
    //  release
    String transformExpression = fieldSpec.getTransformFunction();
    if (transformExpression != null && !transformExpression.isEmpty()) {

      // if transform function expression present, use it to generate function evaluator
      try {
        functionEvaluator = getExpressionEvaluator(transformExpression, ingestionGroovyDisabled);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Caught exception while constructing expression evaluator for transform expression: " + transformExpression
                + " of column: " + columnName + ", exception: " + e.getMessage(), e);
      }
    } else if (fieldSpec.getFieldType() == FieldSpec.FieldType.TIME) {

      // Time conversions should be done using DateTimeFieldSpec and transformFunctions
      // But we need below lines for converting TimeFieldSpec's incoming to outgoing
      TimeFieldSpec timeFieldSpec = (TimeFieldSpec) fieldSpec;
      TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
      TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();
      if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
        if (!incomingGranularitySpec.getName().equals(outgoingGranularitySpec.getName())) {
          functionEvaluator = new TimeSpecFunctionEvaluator(incomingGranularitySpec, outgoingGranularitySpec);
        } else {
          throw new IllegalStateException(
              "Invalid timeSpec - Incoming and outgoing field specs are different, but name " + incomingGranularitySpec
                  .getName() + " is same");
        }
      }
    } else if (columnName.endsWith(MAP_KEY_COLUMN_SUFFIX)) {

      // for backward compatible handling of Map type (currently only in Avro)
      String sourceMapName = columnName.substring(0, columnName.length() - MAP_KEY_COLUMN_SUFFIX.length());
      functionEvaluator = new MapFunctionEvaluator(sourceMapName, true);
    } else if (columnName.endsWith(MAP_VALUE_COLUMN_SUFFIX)) {
      // for backward compatible handling of Map type in avro (currently only in Avro)
      String sourceMapName =
          columnName.substring(0, columnName.length() - MAP_VALUE_COLUMN_SUFFIX.length());
      functionEvaluator = new MapFunctionEvaluator(sourceMapName, false);
    }
    return functionEvaluator;
  }

  public static FunctionEvaluator getExpressionEvaluator(String transformExpression) {
    return getExpressionEvaluator(transformExpression, _ingestionGroovyDisabled);
  }

  /// Creates an ingestion evaluator using an explicit Groovy policy.
  public static FunctionEvaluator getExpressionEvaluator(String transformExpression, boolean ingestionGroovyDisabled) {
    validateIngestionGroovyPolicy(transformExpression, ingestionGroovyDisabled);
    if (isGroovyExpression(transformExpression)) {
      return new GroovyFunctionEvaluator(transformExpression);
    } else {
      return new InbuiltFunctionEvaluator(transformExpression);
    }
  }

  /// Sets whether Groovy-backed ingestion evaluators are disabled process-wide.
  public static void setIngestionGroovyDisabled(boolean ingestionGroovyDisabled) {
    _ingestionGroovyDisabled = ingestionGroovyDisabled;
  }

  /// Reads the ingestion Groovy policy from a configuration string. Only an explicit `false` enables Groovy.
  public static boolean resolveIngestionGroovyDisabled(@Nullable String configuredValue) {
    return CommonConstants.Groovy.isIngestionGroovyDisabled(configuredValue);
  }

  /// Returns whether Groovy-backed ingestion evaluators are disabled process-wide.
  public static boolean isIngestionGroovyDisabled() {
    return _ingestionGroovyDisabled;
  }

  // Visible for tests in the same package.
  static void resetIngestionGroovyDisabledFromSystemProperty() {
    _ingestionGroovyDisabled = readIngestionGroovyDisabledFromSystemProperty();
  }

  /// Rejects a Groovy ingestion expression when Groovy is disabled. Built-in expressions are always allowed.
  public static void validateIngestionGroovyPolicy(String transformExpression) {
    validateIngestionGroovyPolicy(transformExpression, _ingestionGroovyDisabled);
  }

  /// Rejects a Groovy ingestion expression under the given explicit policy.
  public static void validateIngestionGroovyPolicy(String transformExpression, boolean ingestionGroovyDisabled) {
    if (ingestionGroovyDisabled && isGroovyExpression(transformExpression)) {
      throw new IllegalStateException(DISABLED_GROOVY_MESSAGE);
    }
  }

  /// @return true if the given transform function is a groovy expression, otherwise returns false
  public static boolean isGroovyExpression(String transformExpression) {
    String groovyPrefix = GroovyFunctionEvaluator.getGroovyExpressionPrefix();
    return transformExpression.regionMatches(true, 0, groovyPrefix, 0, groovyPrefix.length());
  }

  private static boolean readIngestionGroovyDisabledFromSystemProperty() {
    String disableGroovy = System.getProperty(CommonConstants.Groovy.DISABLE_INGESTION_GROOVY);
    if (disableGroovy == null) {
      return CommonConstants.Groovy.DEFAULT_DISABLE_INGESTION_GROOVY;
    }
    return resolveIngestionGroovyDisabled(disableGroovy);
  }

  private static class MapFunctionEvaluator implements FunctionEvaluator {
    private final String _mapColumnName;
    private final List<String> _arguments;
    private final boolean _extractKeys;

    MapFunctionEvaluator(String mapColumnName, boolean extractKeys) {
      _mapColumnName = mapColumnName;
      _arguments = List.of(mapColumnName);
      _extractKeys = extractKeys;
    }

    @Override
    public List<String> getArguments() {
      return _arguments;
    }

    @Override
    public Object evaluate(GenericRow genericRow) {
      return evaluateMap(genericRow.getValue(_mapColumnName));
    }

    @Override
    public Object evaluate(Object[] values) {
      return evaluateMap(values[0]);
    }

    @Nullable
    private List<Object> evaluateMap(@Nullable Object value) {
      if (value == null) {
        return null;
      }

      Map<?, ?> map = (Map<?, ?>) value;
      Map<?, ?> sortedMap = new TreeMap<>(map);
      List<Object> result = new ArrayList<>(sortedMap.size());
      for (Map.Entry<?, ?> entry : sortedMap.entrySet()) {
        result.add(_extractKeys ? entry.getKey() : entry.getValue());
      }
      return result;
    }
  }
}
