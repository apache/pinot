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
package org.apache.pinot.plugin.inputformat.avro;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of StreamMessageDecoder to read avro records from pulsar
 */
public class PulsarAvroMessageDecoder extends SimpleAvroMessageDecoder {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarAvroMessageDecoder.class);

  private static final String PULSAR_ADMIN_URL = "admin.servers";

  private List<String> _microsFieldNames = new ArrayList<>();

  @Override
  protected void initAvroSchema(Map<String, String> props, String topicName) {
    Preconditions.checkState(props.containsKey(PULSAR_ADMIN_URL), "Pulsar admin url must be provided");
    String adminUrl = props.get(PULSAR_ADMIN_URL);
    String topic = TopicName.get(topicName).getPartitionedTopicName();
    SchemaInfo schemaInfo;
    try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
      schemaInfo = admin.schemas().getSchemaInfo(topic);
    } catch (Exception e) {
      LOGGER.error("[{}] get schema info failed.", topic);
      throw new RuntimeException("Get schema info failed", e);
    }
    if (schemaInfo.getType() != SchemaType.AVRO) {
      LOGGER.error("[{}] schema type is {}, current decoder only support avro.", topic, schemaInfo.getType());
      throw new RuntimeException("UnSupport schema type in PulsarAvroMessageDecoder");
    }
    _avroSchema = new org.apache.avro.Schema.Parser().parse(schemaInfo.getSchemaDefinition());

    if (_avroSchema != null) {
      for (Schema.Field field : _avroSchema.getFields()) {
        Schema nonNullSchema = getNonNullSchema(field.schema());
        if (nonNullSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros
            || nonNullSchema.getLogicalType() instanceof LogicalTypes.LocalTimestampMicros
            || nonNullSchema.getLogicalType() instanceof LogicalTypes.TimeMicros) {
          _microsFieldNames.add(field.name());
        }
      }
    }
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    _binaryDecoderToReuse = DecoderFactory.get().binaryDecoder(payload, offset, length, _binaryDecoderToReuse);
    try {
      _avroRecordToReuse = _datumReader.read(_avroRecordToReuse, _binaryDecoderToReuse);
      for (String name : _microsFieldNames) {
        // convert micros to millis
        _avroRecordToReuse.put(name, ((long) _avroRecordToReuse.get(name)) / 1000);
      }
    } catch (IOException e) {
      LOGGER.error("Caught exception while reading message using schema: {}", _avroSchema, e);
      return null;
    }
    return _avroRecordExtractor.extract(_avroRecordToReuse, destination);
  }

  private Schema getNonNullSchema(Schema fieldSchema) {
    if (fieldSchema.getType().equals(Schema.Type.UNION)) {
      List<Schema> types = fieldSchema.getTypes();
      for (int i = types.size() - 1; i >= 0; i--) {
        Schema type = types.get(i);
        if (!type.isNullable()) {
          return type;
        }
      }
    }
    return fieldSchema;
  }
}
