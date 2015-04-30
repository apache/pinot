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
package com.linkedin.pinot.core.indexsegment.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;


/**
 *
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 19, 2014
 */
public class AvroUtils {

  /**
   * gives back a basic pinot schema object with field type as unknown and not aware of whether SV or MV
   * this is just a util method for testing
   * @param avroFile
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static Schema extractSchemaFromAvro(File avroFile) throws FileNotFoundException, IOException {
    final Schema schema = new Schema();
    final DataFileStream<GenericRecord> dataStreamReader = getAvroReader(avroFile);
    final org.apache.avro.Schema avroSchema = dataStreamReader.getSchema();
    for (final Field field : avroSchema.getFields()) {
      FieldSpec spec;
      FieldSpec.DataType columnType = AvroRecordReader.getColumnType(field);

      if (field.name().contains("count") || field.name().contains("met")) {
        spec = new MetricFieldSpec();
        spec.setDataType(columnType);
      } else if (field.name().contains("day") || field.name().equalsIgnoreCase("daysSinceEpoch")) {
        spec = new TimeFieldSpec(field.name(), columnType, TimeUnit.DAYS);
      } else {
        spec = new DimensionFieldSpec();
        spec.setDataType(columnType);
      }

      spec.setName(field.name());
      spec.setSingleValueField(AvroRecordReader.isSingleValueField(field));

      schema.addSchema(spec.getName(), spec);
    }
    dataStreamReader.close();
    return schema;
  }

  public static List<String> getAllColumnsInAvroFile(File avroFile) throws FileNotFoundException, IOException {
    final List<String> ret = new ArrayList<String>();
    final DataFileStream<GenericRecord> reader = getAvroReader(avroFile);
    for (final Field f : reader.getSchema().getFields()) {
      ret.add(f.name());
    }
    reader.close();
    return ret;
  }

  public static DataFileStream<GenericRecord> getAvroReader(File avroFile) throws FileNotFoundException, IOException {
    if(avroFile.getName().endsWith("gz"))
      return new DataFileStream<GenericRecord>(new GZIPInputStream(new FileInputStream(avroFile)), new GenericDatumReader<GenericRecord>());
    else
      return new DataFileStream<GenericRecord>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
  }

}
