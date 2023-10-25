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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * The class for transforming validating GenericRow data against table schema and table config.
 * It is used mainly but not limited by RealTimeDataManager for each row that is going to be indexed into Pinot.
 */
public class TransformPipeline {
  private final RecordTransformer _recordTransformer;
  private final ComplexTypeTransformer _complexTypeTransformer;

  /**
   * Constructs a transform pipeline with customized RecordTransformer and customized ComplexTypeTransformer
   * @param recordTransformer the customized record transformer
   * @param complexTypeTransformer the customized complexType transformer
   */
  public TransformPipeline(RecordTransformer recordTransformer,
      @Nullable ComplexTypeTransformer complexTypeTransformer) {
    _recordTransformer = recordTransformer;
    _complexTypeTransformer = complexTypeTransformer;
  }

  /**
   * Constructs a transform pipeline based on TableConfig and table schema.
   * @param tableConfig the config for the table
   * @param schema the table schema
   */
  public TransformPipeline(TableConfig tableConfig, Schema schema) {
    // Create record transformer
    _recordTransformer = CompositeTransformer.composeDefaultTransformers(tableConfig, schema);

    // Create complex type transformer
    _complexTypeTransformer = ComplexTypeTransformer.getComplexTypeTransformer(tableConfig);
  }

  /**
   * Returns a pass through pipeline that does not transform the record.
   */
  public static TransformPipeline getPassThroughPipeline() {
    return new TransformPipeline(CompositeTransformer.getPassThroughTransformer(), null);
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
    if (_complexTypeTransformer != null) {
      // TODO: consolidate complex type transformer into composite type transformer
      decodedRow = _complexTypeTransformer.transform(decodedRow);
    }
    Collection<GenericRow> rows = (Collection<GenericRow>) decodedRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    if (rows != null) {
      for (GenericRow row : rows) {
        processPlainRow(row, reusedResult);
      }
    } else {
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

    public void reset() {
      _skippedRowCount = 0;
      _incompleteRowCount = 0;
      _transformedRows.clear();
    }
  }
}
