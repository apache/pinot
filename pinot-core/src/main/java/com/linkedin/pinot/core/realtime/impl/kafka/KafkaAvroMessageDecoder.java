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
package com.linkedin.pinot.core.realtime.impl.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;


public class KafkaAvroMessageDecoder implements KafkaMessageDecoder {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroMessageDecoder.class);

  public static final String SCHEMA_REGISTRY_REST_URL = "schema.registry.rest.url";
  public static final String SCHEMA_REGISTRY_SCHEMA_NAME = "schema.registry.schema.name";
  private org.apache.avro.Schema defaultAvroSchema;
  private Map<String, org.apache.avro.Schema> md5ToAvroSchemaMap;

  private String schemaRegistryBaseUrl;
  private String kafkaTopicName;
  private DecoderFactory decoderFactory;
  private AvroRecordToPinotRowGenerator avroRecordConvetrer;

  private static final int MAGIC_BYTE_LENGTH = 1;
  private static final int SCHEMA_HASH_LENGTH = 16;
  private static final int HEADER_LENGTH = MAGIC_BYTE_LENGTH + SCHEMA_HASH_LENGTH;

  private static final int SCHEMA_HASH_START_OFFSET = MAGIC_BYTE_LENGTH;
  private static final int SCHEMA_HASH_END_OFFSET = SCHEMA_HASH_START_OFFSET + SCHEMA_HASH_LENGTH;

  @Override
  public void init(Map<String, String> props, Schema indexingSchema, String topicName) throws Exception {
    schemaRegistryBaseUrl = props.get(SCHEMA_REGISTRY_REST_URL);
    StringUtils.chomp(schemaRegistryBaseUrl, "/");
    kafkaTopicName = topicName;

    String avroSchemaName = kafkaTopicName;
    if(props.containsKey(SCHEMA_REGISTRY_SCHEMA_NAME) && props.get(SCHEMA_REGISTRY_SCHEMA_NAME) != null &&
        !props.get(SCHEMA_REGISTRY_SCHEMA_NAME).isEmpty()) {
      avroSchemaName = props.get(SCHEMA_REGISTRY_SCHEMA_NAME);
    }

    defaultAvroSchema = fetchSchema(new URL(schemaRegistryBaseUrl + "/latest_with_type=" + avroSchemaName));
    this.avroRecordConvetrer = new AvroRecordToPinotRowGenerator(indexingSchema);
    this.decoderFactory = new DecoderFactory();
    md5ToAvroSchemaMap = new HashMap<String, org.apache.avro.Schema>();
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    return decode(payload, 0, payload.length, destination);
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    if (payload == null || payload.length == 0 || length == 0) {
      return null;
    }

    byte[] md5 = Arrays.copyOfRange(payload, SCHEMA_HASH_START_OFFSET + offset, SCHEMA_HASH_END_OFFSET + offset);

    String md5String = hex(md5);
    org.apache.avro.Schema schema = null;
    if (md5ToAvroSchemaMap.containsKey(md5String)) {
      schema = md5ToAvroSchemaMap.get(md5String);
    } else {
      try {
        schema = fetchSchema(new URL(schemaRegistryBaseUrl + "/id=" + md5String));
        md5ToAvroSchemaMap.put(md5String, schema);
      } catch (Exception e) {
        schema = defaultAvroSchema;
        LOGGER.error("error fetching schema from md5 String", e);
      }
    }
    DatumReader<Record> reader = new GenericDatumReader<Record>(schema);
    try {
      GenericData.Record avroRecord =
          reader.read(null, decoderFactory.createBinaryDecoder(payload, HEADER_LENGTH + offset,
              length - HEADER_LENGTH, null));
      return avroRecordConvetrer.transform(avroRecord, schema, destination);
    } catch (IOException e) {
      LOGGER.error("Caught exception while reading message", e);
      return null;
    }
  }

  public static String hex(byte[] bytes) {
    StringBuilder builder = new StringBuilder(2 * bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      String hexString = Integer.toHexString(0xFF & bytes[i]);
      if (hexString.length() < 2) {
        hexString = "0" + hexString;
      }
      builder.append(hexString);
    }
    return builder.toString();
  }

  public static org.apache.avro.Schema fetchSchema(URL url) throws Exception {
    BufferedReader reader = null;

    reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
    StringBuilder queryResp = new StringBuilder();
    for (String respLine; (respLine = reader.readLine()) != null;) {
      queryResp.append(respLine);
    }
    return org.apache.avro.Schema.parse(queryResp.toString());
  }

}
