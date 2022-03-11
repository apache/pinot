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

  public Result processRow(GenericRow decodedRow) throws Exception {
    Result res = new Result();
    if (_complexTypeTransformer != null) {
      // TODO: consolidate complex type transformer into composite type transformer
      decodedRow = _complexTypeTransformer.transform(decodedRow);
    }
    Collection<GenericRow> rows = (Collection<GenericRow>) decodedRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    if (rows == null) {
      rows = ImmutableList.of(decodedRow);
    }
    for (GenericRow row : rows) {
      GenericRow transformedRow = _recordTransformer.transform(row);
      if (transformedRow != null && IngestionUtils.shouldIngestRow(row)) {
        res.addTransformedRows(transformedRow);
      } else {
        res.addFailedRows(row);
      }
    }
    return res;
  }

  public static class Result {
    private List<GenericRow> _transformedRows = new ArrayList<>();
    private List<GenericRow> _failedRows = new ArrayList<>();

    public List<GenericRow> getTransformedRows() {
      return _transformedRows;
    }

    public List<GenericRow> getFailedRows() {
      return _failedRows;
    }

    public void addTransformedRows(GenericRow row) {
      _transformedRows.add(row);
    }

    public void addFailedRows(GenericRow row) {
      _failedRows.add(row);
    }
  }
}
