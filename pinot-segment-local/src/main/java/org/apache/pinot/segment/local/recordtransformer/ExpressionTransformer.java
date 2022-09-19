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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * The {@code ExpressionTransformer} class will evaluate the function expressions.
 * <p>NOTE: should put this before the {@link DataTypeTransformer}. After this, transformed column can be treated as
 * regular column for other record transformers.
 */
public class ExpressionTransformer implements RecordTransformer {

  @VisibleForTesting
  final LinkedHashMap<String, FunctionEvaluator> _expressionEvaluators = new LinkedHashMap<>();
  private final boolean _continueOnError;

  public ExpressionTransformer(TableConfig tableConfig, Schema schema) {
    Map<String, FunctionEvaluator> expressionEvaluators = new HashMap<>();
    _continueOnError = tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().isContinueOnError();
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getTransformConfigs() != null) {
      for (TransformConfig transformConfig : tableConfig.getIngestionConfig().getTransformConfigs()) {
        FunctionEvaluator previous = expressionEvaluators.put(transformConfig.getColumnName(),
            FunctionEvaluatorFactory.getExpressionEvaluator(transformConfig.getTransformFunction()));
        Preconditions
            .checkState(previous == null, "Cannot set more than one ingestion transform function on column: %s.",
                transformConfig.getColumnName());
      }
    }
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      if (!fieldSpec.isVirtualColumn() && !expressionEvaluators.containsKey(fieldName)) {
        FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
        if (functionEvaluator != null) {
          expressionEvaluators.put(fieldName, functionEvaluator);
        }
      }
    }
    // For fields with Timestamp indexes, also generate the corresponding values during record transformation.
    if (tableConfig.getFieldConfigList() != null) {
      for (FieldConfig fieldConfig : tableConfig.getFieldConfigList()) {
        if (fieldConfig.getIndexTypes().contains(FieldConfig.IndexType.TIMESTAMP)) {
          for (TimestampIndexGranularity granularity : fieldConfig.getTimestampConfig().getGranularities()) {
            expressionEvaluators.put(
                TimestampIndexGranularity.getColumnNameWithGranularity(fieldConfig.getName(), granularity),
                FunctionEvaluatorFactory.getExpressionEvaluator(
                    TimestampIndexGranularity.getTransformExpression(fieldConfig.getName(), granularity)));
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
      throw new IllegalStateException("Expression cycle found for column '" + column + "' in Ingestion Transform "
          + "Function definitions.");
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (Map.Entry<String, FunctionEvaluator> entry : _expressionEvaluators.entrySet()) {
      String column = entry.getKey();
      FunctionEvaluator transformFunctionEvaluator = entry.getValue();
      // Skip transformation if column value already exist.
      // NOTE: column value might already exist for OFFLINE data
      if (record.getValue(column) == null) {
        if (_continueOnError) {
          try {
            record.putValue(column, transformFunctionEvaluator.evaluate(record));
          } catch (Exception e) {
            record.putValue(column, null);
          }
        } else {
          record.putValue(column, transformFunctionEvaluator.evaluate(record));
        }
      }
    }
    return record;
  }
}
