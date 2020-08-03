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
package org.apache.pinot.tools.segment.converter;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.core.data.readers.PinotSegmentRecordReader;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * The <code>PinotSegmentToAvroConverter</code> class is the tool to convert Pinot segment to AVRO format.
 */
public class PinotSegmentToAvroConverter implements PinotSegmentConverter {
  private final String _segmentDir;
  private final String _outputFile;

  public PinotSegmentToAvroConverter(String segmentDir, String outputFile) {
    _segmentDir = segmentDir;
    _outputFile = outputFile;
  }

  @Override
  public void convert()
      throws Exception {
    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(new File(_segmentDir))) {
      Schema avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(pinotSegmentRecordReader.getSchema());

      try (DataFileWriter<Record> recordWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        recordWriter.create(avroSchema, new File(_outputFile));

        GenericRow row = new GenericRow();
        while (pinotSegmentRecordReader.hasNext()) {
          row = pinotSegmentRecordReader.next(row);
          Record record = new Record(avroSchema);

          for (String field : row.getFieldNames()) {
            Object value = row.getValue(field);
            if (value instanceof Object[]) {
              record.put(field, Arrays.asList((Object[]) value));
            } else {
              // PinotSegmentRecordReader returns byte[] instead of ByteBuffer.
              if (value instanceof byte[]) {
                value = ByteBuffer.wrap((byte[]) value);
              }
              record.put(field, value);
            }
          }

          recordWriter.append(record);
        }
      }
    }
  }
}
