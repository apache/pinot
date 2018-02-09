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
package com.linkedin.pinot.tools.data.generator;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;


public class AvroWriter implements Closeable {
  private final Map<String, Generator> _generatorMap;
  private final org.apache.avro.Schema _avroSchema;
  private final DataFileWriter<GenericData.Record> _recordWriter;

  public AvroWriter(File baseDir, int index, Map<String, Generator> generatorMap, Schema schema) throws IOException {
    _generatorMap = generatorMap;
    _avroSchema = getAvroSchema(schema);
    _recordWriter = new DataFileWriter<>(new GenericDatumWriter<GenericData.Record>(_avroSchema));
    _recordWriter.create(_avroSchema, new File(baseDir, "part-" + index + ".avro"));
  }

  public static org.apache.avro.Schema getAvroSchema(Schema schema) {
    JsonObject avroSchema = new JsonObject();
    avroSchema.addProperty("name", "data_gen_record");
    avroSchema.addProperty("type", "record");

    JsonArray fields = new JsonArray();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      JsonObject jsonObject = fieldSpec.toAvroSchemaJsonObject();
      fields.add(jsonObject);
    }
    avroSchema.add("fields", fields);

    return new org.apache.avro.Schema.Parser().parse(avroSchema.toString());
  }

  public void writeNext() throws IOException {
    GenericData.Record nextRecord = new GenericData.Record(_avroSchema);
    for (String column : _generatorMap.keySet()) {
      nextRecord.put(column, _generatorMap.get(column).next());
    }
    _recordWriter.append(nextRecord);
  }

  @Override
  public void close() throws IOException {
    _recordWriter.close();
  }
}
