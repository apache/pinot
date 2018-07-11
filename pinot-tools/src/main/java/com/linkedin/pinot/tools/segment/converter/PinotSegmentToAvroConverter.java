/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.segment.converter;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.util.AvroUtils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.File;
import java.util.Arrays;


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
  public void convert() throws Exception {
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader(new File(_segmentDir))) {
      Schema avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(recordReader.getSchema());

      try (DataFileWriter<Record> recordWriter = new DataFileWriter<>(new GenericDatumWriter<Record>(avroSchema))) {
        recordWriter.create(avroSchema, new File(_outputFile));

        GenericRow row = new GenericRow();
        while (recordReader.hasNext()) {
          row = recordReader.next(row);
          Record record = new Record(avroSchema);

          for (String field : row.getFieldNames()) {
            Object value = row.getValue(field);
            if (value instanceof Object[]) {
              record.put(field, Arrays.asList((Object[]) value));
            } else {
              record.put(field, value);
            }
          }

          recordWriter.append(record);
        }
      }
    }
  }
}
