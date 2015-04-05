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
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;


public class KafkaAvroMessageDecoder implements KafkaMessageDecoder {
  private static final Logger logger = Logger.getLogger(KafkaAvroMessageDecoder.class);

  public static final String SCHEMA_REGISTRY_REST_URL = "schema.registry.rest.url";
  private org.apache.avro.Schema defaultAvroSchema;
  private Map<String, org.apache.avro.Schema> md5ToAvroSchemaMap;

  private String schemaRegistryBaseUrl;
  private String kafkaTopicName;
  private DecoderFactory decoderFactory;
  private AvroRecordToPinotRowGenerator avroRecordConvetrer;

  @Override
  public void init(Map<String, String> props, Schema indexingSchema, String topicName) throws Exception {
    for (String key : props.keySet()) {
      System.out.println(key + ":" + props.get(key));
    }
    schemaRegistryBaseUrl = props.get(SCHEMA_REGISTRY_REST_URL);
    StringUtils.chomp(schemaRegistryBaseUrl, "/");
    kafkaTopicName = topicName;
    defaultAvroSchema = fetchSchema(new URL(schemaRegistryBaseUrl + "/latest_with_type=" + kafkaTopicName));
    this.avroRecordConvetrer = new AvroRecordToPinotRowGenerator(indexingSchema);
    this.decoderFactory = new DecoderFactory();
    md5ToAvroSchemaMap = new HashMap<String, org.apache.avro.Schema>();
  }

  @Override
  public GenericRow decode(byte[] payload) {
    if (payload == null || payload.length == 0) {
      return null;
    }

    // can use the md5 hash to fetch id specific schema
    // will implement that later
    byte[] md5 = new byte[16];
    md5 = Arrays.copyOfRange(payload, 1, 1 + md5.length);

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
        logger.error("error fetching schema from md5 String", e);
      }
    }
    int start = 1 + md5.length;
    int length = payload.length - 1 - md5.length;
    DatumReader<Record> reader = new GenericDatumReader<Record>(schema);
    try {
      GenericData.Record avroRecord =
          reader.read(null, decoderFactory.createBinaryDecoder(payload, start, length, null));
      return avroRecordConvetrer.transform(avroRecord, schema);
    } catch (IOException e) {
      logger.error(e.getMessage());
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
