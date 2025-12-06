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
package org.apache.pinot.segment.local.recordtransformer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.ThrottledLogger;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code ExpressionTransformer} class will evaluate the function expressions.
 * <p>NOTE: should put this before the {@link DataTypeTransformer}. After this, transformed column can be treated as
 * regular column for other record transformers.
 * TODO: Merge this and CustomFunctionEnricher
 */
public class ExpressionTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionTransformer.class);

  @VisibleForTesting
  final LinkedHashMap<String, FunctionEvaluator> _expressionEvaluators = new LinkedHashMap<>();
  private final Set<String> _implicitMapTransformColumns = new HashSet<>();
  private final boolean _continueOnError;
  private final ThrottledLogger _throttledLogger;
  /**
   * If {@code true}, transform functions overwrite existing non-null values instead of skipping them. This is enabled
   * for post-upsert transforms where derived columns should be recomputed on the merged row; otherwise transforms only
   * populate missing or null-valued columns.
   */
  private final boolean _overwriteExistingValues;

  public ExpressionTransformer(TableConfig tableConfig, Schema schema) {
    this(tableConfig, schema, null, false);
  }

  public ExpressionTransformer(TableConfig tableConfig, Schema schema,
      @Nullable List<TransformConfig> transformConfigs, boolean overwriteExistingValues) {
    Map<String, FunctionEvaluator> expressionEvaluators = new HashMap<>();
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    List<TransformConfig> resolvedTransformConfigs =
        transformConfigs != null ? transformConfigs
            : ingestionConfig != null ? ingestionConfig.getTransformConfigs() : null;
    if (resolvedTransformConfigs != null) {
      for (TransformConfig transformConfig : resolvedTransformConfigs) {
        FunctionEvaluator previous = expressionEvaluators.put(transformConfig.getColumnName(),
            FunctionEvaluatorFactory.getExpressionEvaluator(transformConfig.getTransformFunction()));
        Preconditions.checkState(previous == null,
            "Cannot set more than one ingestion transform function on column: %s.", transformConfig.getColumnName());
      }
    }
    if (transformConfigs == null) {
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        String fieldName = fieldSpec.getName();
        if (!fieldSpec.isVirtualColumn() && !expressionEvaluators.containsKey(fieldName)) {
          FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
          if (functionEvaluator != null) {
            expressionEvaluators.put(fieldName, functionEvaluator);
            if (isImplicitMapTransform(fieldSpec)) {
              _implicitMapTransformColumns.add(fieldName);
            }
          }
        }
      }
    }

    // Carry out DFS traversal to topologically sort column names based on transform function dependencies. Throw
    // exception if a cycle is discovered. When a name is first seen it is added to discoveredNames set. When a name
    // is completely processed (i.e the name and all of its dependencies have been fully explored and no cycles have
    // been seen), it gets added to the _expressionEvaluators list in topologically sorted order. Fully explored
    // names are removed from discoveredNames set.
    Set<String> discoveredNames = new HashSet<>();
    for (Map.Entry<String, FunctionEvaluator> entry : expressionEvaluators.entrySet()) {
      String columnName = entry.getKey();
      if (!_expressionEvaluators.containsKey(columnName)) {
        topologicalSort(columnName, expressionEvaluators, discoveredNames);
      }
    }

    _continueOnError = ingestionConfig != null && ingestionConfig.isContinueOnError();
    _throttledLogger = new ThrottledLogger(LOGGER, ingestionConfig);
    _overwriteExistingValues = overwriteExistingValues;
  }

  private void topologicalSort(String column, Map<String, FunctionEvaluator> expressionEvaluators,
      Set<String> discoveredNames) {
    FunctionEvaluator functionEvaluator = expressionEvaluators.get(column);
    if (functionEvaluator == null) {
      return;
    }

    if (discoveredNames.add(column)) {
      List<String> arguments = functionEvaluator.getArguments();
      for (String arg : arguments) {
        if (!_expressionEvaluators.containsKey(arg)) {
          topologicalSort(arg, expressionEvaluators, discoveredNames);
        }
      }
      _expressionEvaluators.put(column, functionEvaluator);
      discoveredNames.remove(column);
    } else {
      throw new IllegalStateException(String.format(
          "Expression cycle found for column '%s' in Ingestion Transform Function definitions.", column));
    }
  }

  @Override
  public boolean isNoOp() {
    return _expressionEvaluators.isEmpty();
  }

  @Override
  public Set<String> getInputColumns() {
    if (_expressionEvaluators.isEmpty()) {
      return Set.of();
    }
    Set<String> inputColumns = new HashSet<>();
    for (Map.Entry<String, FunctionEvaluator> entry : _expressionEvaluators.entrySet()) {
      inputColumns.addAll(entry.getValue().getArguments());
      // NOTE: Add the column itself too, so that if it is already transformed, we won't transform again
      inputColumns.add(entry.getKey());
    }
    return inputColumns;
  }

  @Override
  public void transform(GenericRow record) {
    for (Map.Entry<String, FunctionEvaluator> entry : _expressionEvaluators.entrySet()) {
      String column = entry.getKey();
      FunctionEvaluator transformFunctionEvaluator = entry.getValue();
      Object existingValue = record.getValue(column);
      boolean shouldApplyTransform = _overwriteExistingValues || existingValue == null || record.isNullValue(column);
      if (shouldApplyTransform) {
        try {
          // Apply transformation when overwriting is enabled or when the current value is null.
          // NOTE: column value might already exist for OFFLINE data. For backward compatibility, we may override
          // nested fields like arrays, collections, or maps since they were not included in the record
          // transformation before.
          Object transformedValue = transformFunctionEvaluator.evaluate(record);
          applyTransformedValue(record, column, transformedValue);
        } catch (Exception e) {
          if (!_continueOnError) {
            throw new RuntimeException("Caught exception while evaluation transform function for column: " + column, e);
          }
          _throttledLogger.warn("Caught exception while evaluation transform function for column: " + column, e);
          record.markIncomplete();
        }
      } else if (existingValue.getClass().isArray() || existingValue instanceof Collection
          || existingValue instanceof Map) {
        try {
          Object transformedValue = transformFunctionEvaluator.evaluate(record);
          if (transformedValue == null && _implicitMapTransformColumns.contains(column)) {
            continue;
          }
          // For backward compatibility, The only exception here is that we will override nested field like array,
          // collection or map since they were not included in the record transformation before.
          if (!isTypeCompatible(existingValue, transformedValue)) {
            applyTransformedValue(record, column, transformedValue);
          }
        } catch (Exception e) {
          LOGGER.debug("Caught exception while evaluation transform function for column: {}", column, e);
        }
      }
    }
  }

  private static boolean isImplicitMapTransform(FieldSpec fieldSpec) {
    if (fieldSpec.getTransformFunction() != null) {
      return false;
    }
    String fieldName = fieldSpec.getName();
    return fieldName.endsWith(SchemaUtils.MAP_KEY_COLUMN_SUFFIX)
        || fieldName.endsWith(SchemaUtils.MAP_VALUE_COLUMN_SUFFIX);
  }

  private void applyTransformedValue(GenericRow record, String column, @Nullable Object transformedValue) {
    if (transformedValue != null) {
      record.removeNullValueField(column);
      record.putValue(column, transformedValue);
    } else {
      record.removeValue(column);
      record.addNullValueField(column);
    }
  }

  private boolean isTypeCompatible(Object existingValue, @Nullable Object transformedValue) {
    if (transformedValue == null || existingValue == null) {
      return transformedValue == existingValue;
    }
    if (transformedValue.getClass() == existingValue.getClass()) {
      return true;
    }
    if (transformedValue instanceof Collection && existingValue instanceof Collection) {
      return true;
    }
    if (transformedValue instanceof Map && existingValue instanceof Map) {
      return true;
    }
    return transformedValue.getClass().isArray() && existingValue.getClass().isArray();
  }
}
