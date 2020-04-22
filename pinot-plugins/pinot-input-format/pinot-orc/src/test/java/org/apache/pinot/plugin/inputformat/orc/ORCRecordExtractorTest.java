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
package org.apache.pinot.plugin.inputformat.orc;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.StringUtils;


/**
 * Tests the {@link ORCRecordReader} using a schema containing groovy transform functions
 */
public class ORCRecordExtractorTest extends AbstractRecordExtractorTest {
  private final File _dataFile = new File(_tempDir, "events.orc");

  /**
   * Create an ORCRecordReader
   */
  @Override
  protected RecordReader createRecordReader()
      throws IOException {
    ORCRecordReader orcRecordReader = new ORCRecordReader();
    orcRecordReader.init(_dataFile, _pinotSchema, null);
    return orcRecordReader;
  }

  @Override
  protected RecordExtractor createRecordExtractor(Set<String> sourceFields) {
    ORCRecordExtractor orcRecordExtractor = new ORCRecordExtractor();
    orcRecordExtractor.init(sourceFields, null);
    return orcRecordExtractor;
  }

  /**
   * Create an ORC input file using the input records
   */
  @Override
  protected void createInputFile()
      throws IOException {
    TypeDescription schema = TypeDescription.fromString(
        "struct<user_id:int,firstName:string,lastName:string,bids:array<int>,campaignInfo:string,cost:double,timestamp:bigint>");
    Writer writer = OrcFile.createWriter(new Path(_dataFile.getAbsolutePath()),
        OrcFile.writerOptions(new Configuration()).setSchema(schema));

    int numRecords = _inputRecords.size();
    VectorizedRowBatch rowBatch = schema.createRowBatch(numRecords);
    LongColumnVector userIdVector = (LongColumnVector) rowBatch.cols[0];
    userIdVector.noNulls = false;
    BytesColumnVector firstNameVector = (BytesColumnVector) rowBatch.cols[1];
    firstNameVector.noNulls = false;
    BytesColumnVector lastNameVector = (BytesColumnVector) rowBatch.cols[2];
    ListColumnVector bidsVector = (ListColumnVector) rowBatch.cols[3];
    bidsVector.noNulls = false;
    LongColumnVector bidsElementVector = (LongColumnVector) bidsVector.child;
    bidsElementVector.ensureSize(6, false);
    BytesColumnVector campaignInfoVector = (BytesColumnVector) rowBatch.cols[4];
    DoubleColumnVector costVector = (DoubleColumnVector) rowBatch.cols[5];
    LongColumnVector timestampVector = (LongColumnVector) rowBatch.cols[6];

    for (int i = 0; i < numRecords; i++) {
      Map<String, Object> record = _inputRecords.get(i);

      Integer userId = (Integer) record.get("user_id");
      if (userId != null) {
        userIdVector.vector[i] = userId;
      } else {
        userIdVector.isNull[i] = true;
      }
      String firstName = (String) record.get("firstName");
      if (firstName != null) {
        firstNameVector.setVal(i, StringUtils.encodeUtf8(firstName));
      } else {
        firstNameVector.isNull[i] = true;
      }
      lastNameVector.setVal(i, StringUtils.encodeUtf8((String) record.get("lastName")));
      List<Integer> bids = (List<Integer>) record.get("bids");
      if (bids != null) {
        bidsVector.offsets[i] = bidsVector.childCount;
        bidsVector.lengths[i] = bids.size();
        for (int bid : bids) {
          bidsElementVector.vector[bidsVector.childCount++] = bid;
        }
      } else {
        bidsVector.isNull[i] = true;
      }
      campaignInfoVector.setVal(i, StringUtils.encodeUtf8((String) record.get("campaignInfo")));
      costVector.vector[i] = (double) record.get("cost");
      timestampVector.vector[i] = (long) record.get("timestamp");

      rowBatch.size++;
    }

    writer.addRowBatch(rowBatch);
    rowBatch.reset();
    writer.close();
  }
}
