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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
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

  private final String _filterFunction;
  private final FunctionEvaluator _evaluator;
  private final boolean _continueOnError;

  private long _numRecordsFiltered;

  public FilterTransformer(TableConfig tableConfig) {
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig != null && ingestionConfig.getFilterConfig() != null) {
      _filterFunction = ingestionConfig.getFilterConfig().getFilterFunction();
    } else {
      _filterFunction = null;
    }
    _evaluator = _filterFunction != null ? FunctionEvaluatorFactory.getExpressionEvaluator(_filterFunction) : null;
    _continueOnError = ingestionConfig != null && ingestionConfig.isContinueOnError();
  }

  @Override
  public boolean isNoOp() {
    return _evaluator == null;
  }

  @Override
  public List<String> getInputColumns() {
    assert _evaluator != null;
    return _evaluator.getArguments();
  }

  @Override
  public List<GenericRow> transform(List<GenericRow> records) {
    assert _evaluator != null;
    List<GenericRow> filteredRecords = new ArrayList<>();
    for (GenericRow record : records) {
      try {
        if (!Boolean.TRUE.equals(_evaluator.evaluate(record))) {
          filteredRecords.add(record);
        } else {
          _numRecordsFiltered++;
        }
      } catch (Exception e) {
        if (!_continueOnError) {
          throw new RuntimeException(
              String.format("Caught exception while executing filter function: %s for record: %s", _filterFunction,
                  record.toString()), e);
        } else {
          LOGGER.debug("Caught exception while executing filter function: {} for record: {}", _filterFunction,
              record.toString(), e);
          record.markIncomplete();
        }
      }
    }
    return filteredRecords;
  }

  public long getNumRecordsFiltered() {
    return _numRecordsFiltered;
  }
}
