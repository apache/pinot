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
package org.apache.pinot.segment.local.segment.creator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.recordtransformer.FilterTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformerUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class for transforming validating GenericRow data against table schema and table config.
 * It is used mainly but not limited by RealTimeDataManager for each row that is going to be indexed into Pinot.
 */
public class TransformPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformPipeline.class);

  private final String _tableNameWithType;
  private final List<RecordTransformer> _transformers;
  @Nullable
  private final FilterTransformer _filterTransformer;

  private long _numRowsProcessed;
  private long _numRowsFiltered;
  private long _numRowsIncomplete;
  private long _numRowsSanitized;

  public TransformPipeline(String tableNameWithType, List<RecordTransformer> transformers) {
    _tableNameWithType = tableNameWithType;
    _transformers = transformers;
    FilterTransformer filterTransformer = null;
    for (RecordTransformer recordTransformer : transformers) {
      if (recordTransformer instanceof FilterTransformer) {
        filterTransformer = (FilterTransformer) recordTransformer;
        break;
      }
    }
    _filterTransformer = filterTransformer;
  }

  public TransformPipeline(TableConfig tableConfig, Schema schema) {
    this(tableConfig.getTableName(), RecordTransformerUtils.getDefaultTransformers(tableConfig, schema));
  }

  public static TransformPipeline getPassThroughPipeline(String tableNameWithType) {
    return new TransformPipeline(tableNameWithType, List.of());
  }

  public Set<String> getInputColumns() {
    Set<String> inputColumns = new HashSet<>();
    for (RecordTransformer transformer : _transformers) {
      inputColumns.addAll(transformer.getInputColumns());
    }
    return inputColumns;
  }

  public Result processRow(GenericRow decodedRow) {
    if (Boolean.TRUE.equals(decodedRow.getValue(GenericRow.SKIP_RECORD_KEY))) {
      return new Result(List.of(), 0, 0, 0);
    }
    //noinspection unchecked
    List<GenericRow> rows = (List<GenericRow>) decodedRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    if (rows == null) {
      rows = List.of(decodedRow);
    }
    _numRowsProcessed += rows.size();
    for (RecordTransformer transformer : _transformers) {
      rows = transformer.transform(rows);
    }
    int skippedRowCount = 0;
    if (_filterTransformer != null) {
      long numRowsFiltered = _filterTransformer.getNumRecordsFiltered();
      skippedRowCount = (int) (numRowsFiltered - _numRowsFiltered);
      _numRowsFiltered = numRowsFiltered;
    }
    int incompleteRowCount = 0;
    int sanitizedRowCount = 0;
    for (GenericRow record : rows) {
      if (record.isIncomplete()) {
        incompleteRowCount++;
        _numRowsIncomplete++;
      }
      if (record.isSanitized()) {
        sanitizedRowCount++;
        _numRowsSanitized++;
      }
    }
    return new Result(rows, skippedRowCount, incompleteRowCount, sanitizedRowCount);
  }

  /// Reports stats after all rows are processed.
  public void reportStats() {
    for (RecordTransformer transformer : _transformers) {
      transformer.reportStats();
    }
    LOGGER.info(
        "Finished processing {} rows (filtered: {}, incomplete: {}, sanitized: {}) with {} transformers for table: {}",
        _numRowsProcessed, _numRowsFiltered, _numRowsIncomplete, _numRowsSanitized, _transformers.size(),
        _tableNameWithType);
  }

  /**
   * Wrapper for transforming results. For efficiency, right now the failed rows have only a counter
   */
  public static class Result {
    private final List<GenericRow> _transformedRows;
    private final int _incompleteRowCount;
    private final int _skippedRowCount;
    private final int _sanitizedRowCount;

    private Result(List<GenericRow> transformedRows, int incompleteRowCount, int skippedRowCount,
        int sanitizedRowCount) {
      _transformedRows = transformedRows;
      _incompleteRowCount = incompleteRowCount;
      _skippedRowCount = skippedRowCount;
      _sanitizedRowCount = sanitizedRowCount;
    }

    public List<GenericRow> getTransformedRows() {
      return _transformedRows;
    }

    public int getIncompleteRowCount() {
      return _incompleteRowCount;
    }

    public int getSkippedRowCount() {
      return _skippedRowCount;
    }

    public int getSanitizedRowCount() {
      return _sanitizedRowCount;
    }
  }
}
