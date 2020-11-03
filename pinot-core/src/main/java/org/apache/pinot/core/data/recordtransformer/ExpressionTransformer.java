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

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.TopologicalOrderIterator;
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

  private final Map<String, FunctionEvaluator> _expressionEvaluators = new HashMap<>();

  /** the order of column to be evaluated */
  private final List<String> _ordered;


  public ExpressionTransformer(TableConfig tableConfig, Schema schema) {
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getTransformConfigs() != null) {
      for (TransformConfig transformConfig : tableConfig.getIngestionConfig().getTransformConfigs()) {
        _expressionEvaluators.put(transformConfig.getColumnName(),
            FunctionEvaluatorFactory.getExpressionEvaluator(transformConfig.getTransformFunction()));
      }
    }
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      if (!fieldSpec.isVirtualColumn() && !_expressionEvaluators.containsKey(fieldName)) {
        FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
        if (functionEvaluator != null) {
          _expressionEvaluators.put(fieldName, functionEvaluator);
        }
      }
    }

    // compute the dependencies of each column to be evaluated
    final DirectedGraph<String, DefaultEdge> columnDependencies = DefaultDirectedGraph.create();

    for (Map.Entry<String, FunctionEvaluator> entry: _expressionEvaluators.entrySet()) {
      for (String arg:entry.getValue().getArguments()) {
        columnDependencies.addVertex(entry.getKey());
        columnDependencies.addVertex(arg);
        // an edge (u -> v) means u needs to happen before v (v depends on u)
        columnDependencies.addEdge(arg, entry.getKey());
      }
    }
    _ordered = ImmutableList.copyOf(new TopologicalOrderIterator<>(columnDependencies));
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (String column : _ordered) {
      FunctionEvaluator transformFunctionEvaluator = _expressionEvaluators.get(column);
      // if not column to be evaluated
      if (transformFunctionEvaluator == null) {
        continue;
      }
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
