package org.apache.pinot.plugin.inputformat.orc;

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

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.pinot.spi.data.readers.AbstractRecordReaderTest;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.StringUtils;


public class ORCRecordReaderTest extends AbstractRecordReaderTest {
  private final File _dataFile = new File(_tempDir, "data.orc");

  @Override
  protected RecordReader createRecordReader()
      throws Exception {
    ORCRecordReader orcRecordReader = new ORCRecordReader();
    orcRecordReader.init(_dataFile, _sourceFields, null);
    return orcRecordReader;
  }

  @Override
  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {
    TypeDescription schema = TypeDescription.fromString(
        "struct<dim_sv_int:int,dim_sv_long:bigint,dim_sv_float:float,dim_sv_double:double,dim_sv_string:string,"
            + "dim_mv_int:array<int>,dim_mv_long:array<bigint>,dim_mv_float:array<float>,dim_mv_double:array<double>,"
            + "dim_mv_string:array<string>,met_int:int,met_long:bigint,met_float:float,met_double:double,"
            + "extra_field:struct<f1:int,f2:int>>");
    Writer writer = OrcFile.createWriter(new Path(_dataFile.getAbsolutePath()),
        OrcFile.writerOptions(new Configuration()).setSchema(schema));

    VectorizedRowBatch rowBatch = schema.createRowBatch();
    int numRowsPerBatch = rowBatch.getMaxSize();
    LongColumnVector dimSVIntVector = (LongColumnVector) rowBatch.cols[0];
    LongColumnVector dimSVLongVector = (LongColumnVector) rowBatch.cols[1];
    DoubleColumnVector dimSVFloatVector = (DoubleColumnVector) rowBatch.cols[2];
    DoubleColumnVector dimSVDoubleVector = (DoubleColumnVector) rowBatch.cols[3];
    BytesColumnVector dimSVStringVector = (BytesColumnVector) rowBatch.cols[4];
    // At most 50 entries for each multi-value
    int maxNumElements = numRowsPerBatch * 50;
    ListColumnVector dimMVIntVector = (ListColumnVector) rowBatch.cols[5];
    LongColumnVector dimMVIntElementVector = (LongColumnVector) dimMVIntVector.child;
    dimMVIntElementVector.ensureSize(maxNumElements, false);
    ListColumnVector dimMVLongVector = (ListColumnVector) rowBatch.cols[6];
    LongColumnVector dimMVLongElementVector = (LongColumnVector) dimMVLongVector.child;
    dimMVLongElementVector.ensureSize(maxNumElements, false);
    ListColumnVector dimMVFloatVector = (ListColumnVector) rowBatch.cols[7];
    DoubleColumnVector dimMVFloatElementVector = (DoubleColumnVector) dimMVFloatVector.child;
    dimMVFloatElementVector.ensureSize(maxNumElements, false);
    ListColumnVector dimMVDoubleVector = (ListColumnVector) rowBatch.cols[8];
    DoubleColumnVector dimMVDoubleElementVector = (DoubleColumnVector) dimMVDoubleVector.child;
    dimMVDoubleElementVector.ensureSize(maxNumElements, false);
    ListColumnVector dimMVStringVector = (ListColumnVector) rowBatch.cols[9];
    BytesColumnVector dimMVStringElementVector = (BytesColumnVector) dimMVStringVector.child;
    dimMVStringElementVector.ensureSize(maxNumElements, false);
    LongColumnVector metIntVector = (LongColumnVector) rowBatch.cols[10];
    LongColumnVector metLongVector = (LongColumnVector) rowBatch.cols[11];
    DoubleColumnVector metFloatVector = (DoubleColumnVector) rowBatch.cols[12];
    DoubleColumnVector metDoubleVector = (DoubleColumnVector) rowBatch.cols[13];

    for (Map<String, Object> record : recordsToWrite) {
      int rowId = rowBatch.size++;

      dimSVIntVector.vector[rowId] = (int) record.get("dim_sv_int");
      dimSVLongVector.vector[rowId] = (long) record.get("dim_sv_long");
      dimSVFloatVector.vector[rowId] = (float) record.get("dim_sv_float");
      dimSVDoubleVector.vector[rowId] = (double) record.get("dim_sv_double");
      dimSVStringVector.setVal(rowId, StringUtils.encodeUtf8((String) record.get("dim_sv_string")));

      List dimMVInts = (List) record.get("dim_mv_int");
      dimMVIntVector.offsets[rowId] = dimMVIntVector.childCount;
      dimMVIntVector.lengths[rowId] = dimMVInts.size();
      for (Object element : dimMVInts) {
        dimMVIntElementVector.vector[dimMVIntVector.childCount++] = (int) element;
      }
      List dimMVLongs = (List) record.get("dim_mv_long");
      dimMVLongVector.offsets[rowId] = dimMVLongVector.childCount;
      dimMVLongVector.lengths[rowId] = dimMVLongs.size();
      for (Object element : dimMVLongs) {
        dimMVLongElementVector.vector[dimMVLongVector.childCount++] = (long) element;
      }
      List dimMVFloats = (List) record.get("dim_mv_float");
      dimMVFloatVector.offsets[rowId] = dimMVFloatVector.childCount;
      dimMVFloatVector.lengths[rowId] = dimMVFloats.size();
      for (Object element : dimMVFloats) {
        dimMVFloatElementVector.vector[dimMVFloatVector.childCount++] = (float) element;
      }
      List dimMVDoubles = (List) record.get("dim_mv_double");
      dimMVDoubleVector.offsets[rowId] = dimMVDoubleVector.childCount;
      dimMVDoubleVector.lengths[rowId] = dimMVDoubles.size();
      for (Object element : dimMVDoubles) {
        dimMVDoubleElementVector.vector[dimMVDoubleVector.childCount++] = (double) element;
      }
      List dimMVStrings = (List) record.get("dim_mv_string");
      dimMVStringVector.offsets[rowId] = dimMVStringVector.childCount;
      dimMVStringVector.lengths[rowId] = dimMVStrings.size();
      for (Object element : dimMVStrings) {
        dimMVStringElementVector.setVal(dimMVStringVector.childCount++, StringUtils.encodeUtf8((String) element));
      }

      metIntVector.vector[rowId] = (int) record.get("met_int");
      metLongVector.vector[rowId] = (long) record.get("met_long");
      metFloatVector.vector[rowId] = (float) record.get("met_float");
      metDoubleVector.vector[rowId] = (double) record.get("met_double");

      if (rowBatch.size == numRowsPerBatch) {
        writer.addRowBatch(rowBatch);
        rowBatch.reset();
      }
    }
    if (rowBatch.size != 0) {
      writer.addRowBatch(rowBatch);
      rowBatch.reset();
    }
    writer.close();
  }
}
