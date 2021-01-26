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
package org.apache.pinot.core.data.recordtransformer;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.function.FunctionEvaluator;
import org.apache.pinot.core.data.function.FunctionEvaluatorFactory;


/**
 * The {@code ExpressionTransformer} class will evaluate the function expressions.
 * <p>NOTE: should put this before the {@link DataTypeTransformer}. After this, transformed column can be treated as
 * regular column for other record transformers.
 */
public class ExpressionTransformer implements RecordTransformer {

  private final LinkedHashMap<String, FunctionEvaluator> _expressionEvaluators = new LinkedHashMap<>();

  public ExpressionTransformer(TableConfig tableConfig, Schema schema) {
    Map<String, FunctionEvaluator> expressionEvaluators = new HashMap<>();
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getTransformConfigs() != null) {
      for (TransformConfig transformConfig : tableConfig.getIngestionConfig().getTransformConfigs()) {
        expressionEvaluators.put(transformConfig.getColumnName(),
            FunctionEvaluatorFactory.getExpressionEvaluator(transformConfig.getTransformFunction()));
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

    // Sort the transform functions based on dependencies
    Set<String> visited = new HashSet<>();
    for (Map.Entry<String, FunctionEvaluator> entry : expressionEvaluators.entrySet()) {
      topologicalSort(entry.getKey(), expressionEvaluators, visited);
    }
  }

  private void topologicalSort(String column, Map<String, FunctionEvaluator> expressionEvaluators, Set<String> visited) {
    if (visited.contains(column)) {
      return;
    }
    FunctionEvaluator functionEvaluator = expressionEvaluators.get(column);
    if (functionEvaluator == null) {
      visited.add(column);
      return;
    }
    List<String> arguments = functionEvaluator.getArguments();
    for (String arg : arguments) {
      topologicalSort(arg, expressionEvaluators, visited);
    }
    visited.add(column);
    _expressionEvaluators.put(column, functionEvaluator);
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (Map.Entry<String, FunctionEvaluator> entry : _expressionEvaluators.entrySet()) {
      String column = entry.getKey();
      FunctionEvaluator transformFunctionEvaluator = entry.getValue();
      // Skip transformation if column value already exist.
      // NOTE: column value might already exist for OFFLINE data
      if (record.getValue(column) == null) {
        Object result = transformFunctionEvaluator.evaluate(record);
        record.putValue(column, result);
      }
    }
    return record;
  }
}
