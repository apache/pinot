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
package org.apache.pinot.controller.recommender.data.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import org.apache.pinot.plugin.inputformat.avro.AvroSchemaUtil;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroWriter implements Writer {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroWriter.class);
  private AvroWriterSpec _spec;

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

  @Override
  public void init(WriterSpec spec) {
    _spec = (AvroWriterSpec) spec;
  }

  @Override
  public void write()
      throws IOException {
    final int numPerFiles = (int) (_spec.getTotalDocs() / _spec.getNumFiles());
    for (int i = 0; i < _spec.getNumFiles(); i++) {
      try (AvroRecordAppender appender = new AvroRecordAppender(
          new File(_spec.getBaseDir(), "part-" + i + ".avro"), getAvroSchema(_spec.getSchema()))) {
        for (int j = 0; j < numPerFiles; j++) {
          appender.append(_spec.getGenerator().nextRow());
        }
      }
    }
  }

  @Override
  public void cleanup() {
    File baseDir = new File(_spec.getBaseDir().toURI());
    for (File file : Objects.requireNonNull(baseDir.listFiles())) {
      if (!file.delete()) {
        LOGGER.error("Unable to delete file {}", file.getAbsolutePath());
      }
    }
    if (!baseDir.delete()) {
      LOGGER.error("Unable to delete directory {}", baseDir.getAbsolutePath());
    }
  }
}
