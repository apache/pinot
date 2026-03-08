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
package org.apache.pinot.segment.local.utils;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class ExpressionTransformerUtils {

  private ExpressionTransformerUtils() {
    // Utility class - prevent instantiation
  }

  public static LinkedHashMap<String, FunctionEvaluator> getTopologicallySortedExpressions(
      TableConfig tableConfig, Schema schema) {
    LinkedHashMap<String, FunctionEvaluator> sortedEvaluators = new LinkedHashMap<>();

    Map<String, FunctionEvaluator> expressionEvaluators = new HashMap<>();
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig != null && ingestionConfig.getTransformConfigs() != null) {
      for (TransformConfig transformConfig : ingestionConfig.getTransformConfigs()) {
        FunctionEvaluator previous = expressionEvaluators.put(transformConfig.getColumnName(),
            FunctionEvaluatorFactory.getExpressionEvaluator(transformConfig.getTransformFunction()));
        Preconditions.checkState(previous == null,
            "Cannot set more than one ingestion transform function on column: %s.", transformConfig.getColumnName());
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

    // Carry out DFS traversal to topologically sort column names based on transform function dependencies. Throw
    // exception if a cycle is discovered. When a name is first seen it is added to discoveredNames set. When a name
    // is completely processed (i.e the name and all of its dependencies have been fully explored and no cycles have
    // been seen), it gets added to the sortedEvaluators list in topologically sorted order. Fully explored
    // names are removed from discoveredNames set.
    Set<String> discoveredNames = new HashSet<>();
    for (Map.Entry<String, FunctionEvaluator> entry : expressionEvaluators.entrySet()) {
      String columnName = entry.getKey();
      if (!sortedEvaluators.containsKey(columnName)) {
        topologicalSort(columnName, expressionEvaluators, sortedEvaluators, discoveredNames);
      }
    }

    return sortedEvaluators;
  }

  private static void topologicalSort(String column, Map<String, FunctionEvaluator> allEvaluators,
      Map<String, FunctionEvaluator> sortedEvaluators,
      Set<String> discoveredNames) {
    FunctionEvaluator functionEvaluator = allEvaluators.get(column);
    if (functionEvaluator == null) {
      return;
    }

    if (discoveredNames.add(column)) {
      List<String> arguments = functionEvaluator.getArguments();
      for (String arg : arguments) {
        if (!sortedEvaluators.containsKey(arg)) {
          topologicalSort(arg, allEvaluators, sortedEvaluators, discoveredNames);
        }
      }
      sortedEvaluators.put(column, functionEvaluator);
      discoveredNames.remove(column);
    } else {
      throw new IllegalStateException(
          "Expression cycle found for column '" + column + "' in Ingestion Transform " + "Function definitions.");
    }
  }
}
