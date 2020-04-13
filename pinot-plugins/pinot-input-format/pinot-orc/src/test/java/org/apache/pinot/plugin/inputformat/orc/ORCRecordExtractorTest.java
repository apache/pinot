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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMapredRecordWriter;
import org.apache.orc.mapred.OrcStruct;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.RecordReader;


/**
 * Tests the {@link ORCRecordExtractor} using a schema containing groovy transform functions
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

  /**
   * Create an ORC input file using the input records
   */
  @Override
  protected void createInputFile()
      throws IOException {
    TypeDescription schema = TypeDescription.createStruct();
    schema.addField("userID", TypeDescription.createInt());
    schema.addField("firstName", TypeDescription.createString());
    schema.addField("lastName", TypeDescription.createString());
    TypeDescription typeBids = TypeDescription.createList(TypeDescription.createInt());
    schema.addField("bids", typeBids);
    schema.addField("campaignInfo", TypeDescription.createString());
    schema.addField("cost", TypeDescription.createDouble());
    schema.addField("timestamp", TypeDescription.createLong());

    Writer writer = OrcFile.createWriter(new Path(_dataFile.getAbsolutePath()),
        OrcFile.writerOptions(new Configuration()).setSchema(schema));
    OrcMapredRecordWriter mrRecordWriter = new OrcMapredRecordWriter(writer);
    for (Map<String, Object> inputRecord : _inputRecords) {
      OrcStruct struct = new OrcStruct(schema);
      Integer userID = (Integer) inputRecord.get("userID");
      struct.setFieldValue("userID", userID == null ? null : new IntWritable(userID));
      String firstName = (String) inputRecord.get("firstName");
      struct.setFieldValue("firstName", firstName == null ? null : new Text(firstName));
      struct.setFieldValue("lastName", new Text((String) inputRecord.get("lastName")));
      List<Integer> bids = (List<Integer>) inputRecord.get("bids");
      if (bids != null) {
        OrcList<IntWritable> bidsList = new OrcList<>(typeBids);
        for (Integer bid : bids) {
          bidsList.add(new IntWritable(bid));
        }
        struct.setFieldValue("bids", bidsList);
      } else {
        struct.setFieldValue("bids", null);
      }
      struct.setFieldValue("campaignInfo", new Text((String) inputRecord.get("campaignInfo")));
      struct.setFieldValue("cost", new DoubleWritable((Double) inputRecord.get("cost")));
      struct.setFieldValue("timestamp", new LongWritable((Long) inputRecord.get("timestamp")));
      mrRecordWriter.write(null, struct);
    } mrRecordWriter.close(null);
  }
}
