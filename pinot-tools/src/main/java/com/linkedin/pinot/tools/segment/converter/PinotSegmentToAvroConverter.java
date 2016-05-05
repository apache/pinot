/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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


import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseFieldTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;

/**
 * This converter converts a pinot segment to avro file
 */
public class PinotSegmentToAvroConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentToAvroConverter.class);
  private static final String AVRO_FILE = "part-m-00000.avro";

  private String segmentIndexDir;
  private String outputDir;

  public PinotSegmentToAvroConverter(String segmentIndexDir, String outputDir) {
    this.segmentIndexDir = segmentIndexDir;
    this.outputDir = outputDir;
  }

  public void convert() throws Exception {

    LOGGER.info("Reading segment dir {}", segmentIndexDir);
    PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(new File(segmentIndexDir));

    Schema avroSchema = constructAvroSchemaFromPinotSchema(pinotSegmentRecordReader.getSchema());
    final GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<GenericData.Record>(avroSchema);
    DataFileWriter<GenericData.Record> recordWriter = new DataFileWriter<GenericData.Record>(datum);
    recordWriter.create(avroSchema, new File(outputDir, AVRO_FILE));

    GenericData.Record outputRecord;
    GenericRow row;

    pinotSegmentRecordReader.init();
    while (pinotSegmentRecordReader.hasNext()) {
      outputRecord = new GenericData.Record(avroSchema);
      row = pinotSegmentRecordReader.next();
      for (String fieldName : row.getFieldNames()) {
        outputRecord.put(fieldName, row.getValue(fieldName));
      }
      recordWriter.append(outputRecord);
    }

    LOGGER.info("Writing to avro file at {}", outputDir);
    pinotSegmentRecordReader.close();
    recordWriter.close();
  }

  private Schema constructAvroSchemaFromPinotSchema(com.linkedin.pinot.common.data.Schema schema) {
    Schema avroSchema = null;

    RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("record");
    FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      DataType dataType = fieldSpec.getDataType();
      BaseFieldTypeBuilder<Schema> baseFieldTypeBuilder = fieldAssembler.name(fieldName).type().nullable();
      switch (dataType) {
        case BOOLEAN:
          fieldAssembler = baseFieldTypeBuilder.booleanType().noDefault();
          break;
        case DOUBLE:
          fieldAssembler = baseFieldTypeBuilder.doubleType().noDefault();
          break;
        case FLOAT:
          fieldAssembler = baseFieldTypeBuilder.floatType().noDefault();
          break;
        case INT:
          fieldAssembler = baseFieldTypeBuilder.intType().noDefault();
          break;
        case LONG:
          fieldAssembler = baseFieldTypeBuilder.longType().noDefault();
          break;
        case STRING:
          fieldAssembler = baseFieldTypeBuilder.stringType().noDefault();
          break;
        default:
          break;
      }
    }

    avroSchema = fieldAssembler.endRecord();
    LOGGER.info("Avro Schema {}", avroSchema.toString(true));

    return avroSchema;
  }

}
