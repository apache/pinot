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
package org.apache.pinot.controller.recommender.data.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.plugin.inputformat.avro.AvroSchemaUtil;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


public class AvroWriter implements Closeable {
  private final Map<String, Generator> _generatorMap;
  private final org.apache.avro.Schema _avroSchema;
  private final DataFileWriter<GenericData.Record> _recordWriter;

  public AvroWriter(File baseDir, int index, Map<String, Generator> generatorMap, Schema schema)
      throws IOException {
    _generatorMap = generatorMap;
    _avroSchema = getAvroSchema(schema);
    _recordWriter = new DataFileWriter<>(new GenericDatumWriter<GenericData.Record>(_avroSchema));
    _recordWriter.create(_avroSchema, new File(baseDir, "part-" + index + ".avro"));
  }

  public static org.apache.avro.Schema getAvroSchema(Schema schema) {
    ObjectNode avroSchema = JsonUtils.newObjectNode();
    avroSchema.put("name", "data_gen_record");
    avroSchema.put("type", "record");

    ArrayNode fields = JsonUtils.newArrayNode();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      JsonNode jsonObject = AvroSchemaUtil.toAvroSchemaJsonObject(fieldSpec);
      fields.add(jsonObject);
    }
    avroSchema.set("fields", fields);

    return new org.apache.avro.Schema.Parser().parse(avroSchema.toString());
  }

  public void writeNext()
      throws IOException {
    GenericData.Record nextRecord = new GenericData.Record(_avroSchema);
    for (String column : _generatorMap.keySet()) {
      nextRecord.put(column, _generatorMap.get(column).next());
    }
    _recordWriter.append(nextRecord);
  }

  @Override
  public void close()
      throws IOException {
    _recordWriter.close();
  }
}
