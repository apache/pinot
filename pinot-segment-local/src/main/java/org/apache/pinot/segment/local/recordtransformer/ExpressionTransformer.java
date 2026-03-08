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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.ThrottledLogger;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.utils.ExpressionTransformerUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
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
  final LinkedHashMap<String, FunctionEvaluator> _expressionEvaluators;
  private final boolean _continueOnError;
  private final ThrottledLogger _throttledLogger;

  public ExpressionTransformer(TableConfig tableConfig, Schema schema) {
    _expressionEvaluators = ExpressionTransformerUtils.getTopologicallySortedExpressions(tableConfig, schema);
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    _continueOnError = ingestionConfig != null && ingestionConfig.isContinueOnError();
    _throttledLogger = new ThrottledLogger(LOGGER, ingestionConfig);
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
      if (existingValue == null) {
        try {
          // Skip transformation if column value already exists
          // NOTE: column value might already exist for OFFLINE data,
          // For backward compatibility, The only exception here is that we will override nested field like array,
          // collection or map since they were not included in the record transformation before.
          record.putValue(column, transformFunctionEvaluator.evaluate(record));
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
          // For backward compatibility, The only exception here is that we will override nested field like array,
          // collection or map since they were not included in the record transformation before.
          if (!isTypeCompatible(existingValue, transformedValue)) {
            record.putValue(column, transformedValue);
          }
        } catch (Exception e) {
          LOGGER.debug("Caught exception while evaluation transform function for column: {}", column, e);
        }
      }
    }
  }

  private boolean isTypeCompatible(Object existingValue, Object transformedValue) {
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
