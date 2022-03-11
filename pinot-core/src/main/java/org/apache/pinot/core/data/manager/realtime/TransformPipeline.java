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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


public class TransformPipeline {
  private final RecordTransformer _recordTransformer;
  private final ComplexTypeTransformer _complexTypeTransformer;
  public TransformPipeline(TableConfig tableConfig, Schema schema) {
    // Create record transformer
    _recordTransformer = CompositeTransformer.getDefaultTransformer(tableConfig, schema);

    // Create complex type transformer
    _complexTypeTransformer = ComplexTypeTransformer.getComplexTypeTransformer(tableConfig);
  }

  /**
   * Process and validate the decoded row against schema.
   * @param decodedRow the row data to pass in
   * @return both processed rows and failed rows in a struct.
   * @throws TransformException when data has issues like schema validation. Fetch the partialResult from Exception
   */
  public Result processRow(GenericRow decodedRow) throws TransformException {
    Result res = new Result();
    // to keep track and add to "failedRows" when exception happens
    GenericRow currentRow = null;
    try {
      if (_complexTypeTransformer != null) {
        // TODO: consolidate complex type transformer into composite type transformer
        decodedRow = _complexTypeTransformer.transform(decodedRow);
      }
      Collection<GenericRow> rows = (Collection<GenericRow>) decodedRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
      if (rows == null) {
        rows = ImmutableList.of(decodedRow);
      }
      for (GenericRow row : rows) {
        currentRow = row;
        GenericRow transformedRow = _recordTransformer.transform(row);
        if (transformedRow != null && IngestionUtils.shouldIngestRow(row)) {
          res.addTransformedRows(transformedRow);
        } else {
          res.addFailedRows(row);
        }
      }
      return res;
    } catch (Exception ex) {
      // when exception happens, the current processing row needs to be added to failed list
      res.addFailedRows(currentRow);
      throw new TransformException("Encountered error while processing row", res, ex);
    }
  }

  public static class Result {
    private final List<GenericRow> _transformedRows = new ArrayList<>();
    private final List<GenericRow> _failedRows = new ArrayList<>();

    public List<GenericRow> getTransformedRows() {
      return ImmutableList.copyOf(_transformedRows);
    }

    public List<GenericRow> getFailedRows() {
      return ImmutableList.copyOf(_failedRows);
    }

    public void addTransformedRows(GenericRow row) {
      _transformedRows.add(row);
    }

    public void addFailedRows(GenericRow row) {
      _failedRows.add(row);
    }
  }

  /**
   * The exception contains the reference to a "partial result".
   * It contains records of failed rows, transformed rows
   */
  public static class TransformException extends Exception {
    private final Result _partialResult;
    public TransformException(String message, Result partialResult, Throwable cause) {
      super(message, cause);
      _partialResult = partialResult;
    }

    /**
     * Retrieve the partially processed rows in a batch.
     * @return the Result containing partial results
     */
    public Result getPartialResult() {
      return _partialResult;
    }
  }
}
