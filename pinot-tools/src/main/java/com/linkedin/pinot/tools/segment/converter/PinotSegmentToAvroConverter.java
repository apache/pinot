/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import java.io.File;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;


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
      Schema avroSchema = buildAvroSchemaFromPinotSchema(recordReader.getSchema());

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

  /**
   * Helper method to build Avro schema from Pinot schema.
   *
   * @param pinotSchema Pinot schema.
   * @return Avro schema.
   */
  private Schema buildAvroSchemaFromPinotSchema(com.linkedin.pinot.common.data.Schema pinotSchema) {
    FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record("record").fields();

    for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
      DataType dataType = fieldSpec.getDataType();
      if (fieldSpec.isSingleValueField()) {
        switch (dataType) {
          case INT:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().intType().noDefault();
            break;
          case LONG:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().longType().noDefault();
            break;
          case FLOAT:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().floatType().noDefault();
            break;
          case DOUBLE:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().doubleType().noDefault();
            break;
          case STRING:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().stringType().noDefault();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + dataType);
        }
      } else {
        switch (dataType) {
          case INT:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().array().items().intType().noDefault();
            break;
          case LONG:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().array().items().longType().noDefault();
            break;
          case FLOAT:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().array().items().floatType().noDefault();
            break;
          case DOUBLE:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().array().items().doubleType().noDefault();
            break;
          case STRING:
            fieldAssembler = fieldAssembler.name(fieldSpec.getName()).type().array().items().stringType().noDefault();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + dataType);
        }
      }
    }

    return fieldAssembler.endRecord();
  }
}
