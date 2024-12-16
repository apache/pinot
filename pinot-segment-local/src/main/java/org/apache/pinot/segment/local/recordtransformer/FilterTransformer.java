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

import java.util.List;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Based on filter config, decide whether to skip or allow this record.
 * If record should be skipped, puts a special key in the record.
 */
public class FilterTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(FilterTransformer.class);

  private String _filterFunction;
  private final FunctionEvaluator _evaluator;
  private final boolean _continueOnError;

  public FilterTransformer(TableConfig tableConfig) {
    _filterFunction = null;
    _continueOnError = tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().isContinueOnError();

    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getFilterConfig() != null) {
      _filterFunction = tableConfig.getIngestionConfig().getFilterConfig().getFilterFunction();
    }
    _evaluator = (_filterFunction != null) ? FunctionEvaluatorFactory.getExpressionEvaluator(_filterFunction) : null;
  }

  @Override
  public boolean isNoOp() {
    return _evaluator == null;
  }

  @Override
  public List<String> getInputColumns() {
    return _evaluator != null ? _evaluator.getArguments() : List.of();
  }

  @Override
  public GenericRow transform(GenericRow record) {
    if (_evaluator != null) {
      try {
        Object result = _evaluator.evaluate(record);
        if (Boolean.TRUE.equals(result)) {
          record.putValue(GenericRow.SKIP_RECORD_KEY, true);
        }
      } catch (Exception e) {
        if (!_continueOnError) {
          throw new RuntimeException(
              String.format("Caught exception while executing filter function: %s for record: %s", _filterFunction,
                  record.toString()), e);
        } else {
          LOGGER.debug("Caught exception while executing filter function: {} for record: {}", _filterFunction,
              record.toString(), e);
          record.putValue(GenericRow.INCOMPLETE_RECORD_KEY, true);
        }
      }
    }
    return record;
  }
}
