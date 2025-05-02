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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;


/**
 * The class for transforming validating GenericRow data against table schema and table config.
 * It is used mainly but not limited by RealTimeDataManager for each row that is going to be indexed into Pinot.
 */
public class TransformPipeline {
  private final List<RecordTransformer> _preComplexTypeTransformers;
  private final ComplexTypeTransformer _complexTypeTransformer;
  private final RecordTransformer _recordTransformer;

  /**
   * Constructs a transform pipeline with customized RecordTransformer and customized ComplexTypeTransformer
   * @param preComplexTypeTransformers the list of customized pre-complex type transformers
   * @param complexTypeTransformer the customized complexType transformer
   * @param recordTransformer the customized record transformer
   */
  public TransformPipeline(@Nullable List<RecordTransformer> preComplexTypeTransformers,
      @Nullable ComplexTypeTransformer complexTypeTransformer, RecordTransformer recordTransformer) {
    _preComplexTypeTransformers = preComplexTypeTransformers;
    _complexTypeTransformer = complexTypeTransformer;
    _recordTransformer = recordTransformer;
  }

  /**
   * Constructs a transform pipeline based on TableConfig and table schema.
   * @param tableConfig the config for the table
   * @param schema the table schema
   */
  public TransformPipeline(TableConfig tableConfig, Schema schema) {
    // Create pre complex type transformers
    _preComplexTypeTransformers = CompositeTransformer.getPreComplexTypeTransformers(tableConfig);

    // Create complex type transformer
    _complexTypeTransformer = ComplexTypeTransformer.getComplexTypeTransformer(tableConfig);

    // Create record transformer
    _recordTransformer = CompositeTransformer.getDefaultTransformer(tableConfig, schema);
  }

  /**
   * Returns a pass through pipeline that does not transform the record.
   */
  public static TransformPipeline getPassThroughPipeline() {
    return new TransformPipeline(null, null, CompositeTransformer.getPassThroughTransformer());
  }

  public Collection<String> getInputColumns() {
    if (_preComplexTypeTransformers == null && _complexTypeTransformer == null) {
      return _recordTransformer.getInputColumns();
    }
    Set<String> inputColumns = new HashSet<>(_recordTransformer.getInputColumns());
    if (_preComplexTypeTransformers != null) {
      for (RecordTransformer preComplexTypeTransformer : _preComplexTypeTransformers) {
        inputColumns.addAll(preComplexTypeTransformer.getInputColumns());
      }
    }
    if (_complexTypeTransformer != null) {
      inputColumns.addAll(_complexTypeTransformer.getInputColumns());
    }
    return inputColumns;
  }

  /**
   * Process and validate the decoded row against schema.
   * @param decodedRow the row data to pass in
   * @param reusedResult the reused result so we can reduce objects created for each row
   * @throws Exception when data has issues like schema validation. Fetch the partialResult from Exception
   */
  public void processRow(GenericRow decodedRow, Result reusedResult)
      throws Exception {
    reusedResult.reset();

    if (_preComplexTypeTransformers != null) {
      for (RecordTransformer preComplexTypeTransformer : _preComplexTypeTransformers) {
        decodedRow = preComplexTypeTransformer.transform(decodedRow);
      }
    }

    if (_complexTypeTransformer != null) {
      // TODO: consolidate complex type transformer into composite type transformer
      decodedRow = _complexTypeTransformer.transform(decodedRow);
    }

    Collection<GenericRow> rows = (Collection<GenericRow>) decodedRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);

    if (CollectionUtils.isNotEmpty(rows)) {
      for (GenericRow row : rows) {
        processPlainRow(row, reusedResult);
      }
    } else {
      decodedRow.removeValue(GenericRow.MULTIPLE_RECORDS_KEY);
      processPlainRow(decodedRow, reusedResult);
    }
  }

  private void processPlainRow(GenericRow plainRow, Result reusedResult) {
    GenericRow transformedRow = _recordTransformer.transform(plainRow);
    if (transformedRow != null && IngestionUtils.shouldIngestRow(transformedRow)) {
      reusedResult.addTransformedRows(transformedRow);
      if (Boolean.TRUE.equals(transformedRow.getValue(GenericRow.INCOMPLETE_RECORD_KEY))) {
        reusedResult.incIncompleteRowCount();
      }
      if (Boolean.TRUE.equals(transformedRow.getValue(GenericRow.SANITIZED_RECORD_KEY))) {
        reusedResult.incSanitizedRowCount();
      }
    } else {
      reusedResult.incSkippedRowCount();
    }
  }

  /**
   * Wrapper for transforming results. For efficiency, right now the failed rows have only a counter
   */
  public static class Result {
    private final List<GenericRow> _transformedRows = new ArrayList<>();
    private int _skippedRowCount = 0;
    private int _incompleteRowCount = 0;
    private int _sanitizedRowCount = 0;

    public List<GenericRow> getTransformedRows() {
      return _transformedRows;
    }

    public int getSkippedRowCount() {
      return _skippedRowCount;
    }

    public int getIncompleteRowCount() {
      return _incompleteRowCount;
    }

    public void addTransformedRows(GenericRow row) {
      _transformedRows.add(row);
    }

    public void incSkippedRowCount() {
      _skippedRowCount++;
    }

    public void incIncompleteRowCount() {
      _incompleteRowCount++;
    }

    public void incSanitizedRowCount() {
      _sanitizedRowCount++;
    }

    public int getSanitizedRowCount() {
      return _sanitizedRowCount;
    }

    public void reset() {
      _skippedRowCount = 0;
      _incompleteRowCount = 0;
      _transformedRows.clear();
    }
  }
}
