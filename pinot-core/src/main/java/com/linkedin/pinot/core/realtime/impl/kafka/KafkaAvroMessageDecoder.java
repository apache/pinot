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
import java.util.concurrent.Callable;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.core.data.GenericRow;


public class KafkaAvroMessageDecoder implements KafkaMessageDecoder {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroMessageDecoder.class);

  private static final String SCHEMA_REGISTRY_REST_URL = "schema.registry.rest.url";
  private static final String SCHEMA_REGISTRY_SCHEMA_NAME = "schema.registry.schema.name";
  private org.apache.avro.Schema defaultAvroSchema;
  private Map<String, org.apache.avro.Schema> md5ToAvroSchemaMap;

  private String schemaRegistryBaseUrl;
  private DecoderFactory decoderFactory;
  private AvroRecordToPinotRowGenerator avroRecordConvetrer;

  private static final int MAGIC_BYTE_LENGTH = 1;
  private static final int SCHEMA_HASH_LENGTH = 16;
  private static final int HEADER_LENGTH = MAGIC_BYTE_LENGTH + SCHEMA_HASH_LENGTH;

  private static final int SCHEMA_HASH_START_OFFSET = MAGIC_BYTE_LENGTH;
  private static final int SCHEMA_HASH_END_OFFSET = SCHEMA_HASH_START_OFFSET + SCHEMA_HASH_LENGTH;

  private static final int MAXIMUM_SCHEMA_FETCH_RETRY_COUNT = 5;
  private static final int MINIMUM_SCHEMA_FETCH_RETRY_TIME_MILLIS = 500;
  private static final float SCHEMA_FETCH_RETRY_EXPONENTIAL_BACKOFF_FACTOR = 2.0f;

  @Override
  public void init(Map<String, String> props, Schema indexingSchema, String topicName) throws Exception {
    schemaRegistryBaseUrl = props.get(SCHEMA_REGISTRY_REST_URL);
    StringUtils.chomp(schemaRegistryBaseUrl, "/");

    String avroSchemaName = topicName;
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
    boolean schemaUpdateFailed = false;
    if (md5ToAvroSchemaMap.containsKey(md5String)) {
      schema = md5ToAvroSchemaMap.get(md5String);
    } else {
      final String schemaUri = schemaRegistryBaseUrl + "/id=" + md5String;
      try {
        schema = fetchSchema(new URL(schemaUri));
        md5ToAvroSchemaMap.put(md5String, schema);
      } catch (Exception e) {
        schema = defaultAvroSchema;
        LOGGER.error("Error fetching schema using url {}. Attempting to continue with previous schema", schemaUri, e);
        schemaUpdateFailed = true;
      }
    }
    DatumReader<Record> reader = new GenericDatumReader<Record>(schema);
    try {
      GenericData.Record avroRecord =
          reader.read(null, decoderFactory.createBinaryDecoder(payload, HEADER_LENGTH + offset,
              length - HEADER_LENGTH, null));
      return avroRecordConvetrer.transform(avroRecord, schema, destination);
    } catch (IOException e) {
      LOGGER.error("Caught exception while reading message using schema {}{}", (schema==null ? "null" : schema.getName()),
          (schemaUpdateFailed? "(possibly due to schema update failure)" : ""), e);
      return null;
    }
  }

  private String hex(byte[] bytes) {
    StringBuilder builder = new StringBuilder(2 * bytes.length);
    for (byte aByte : bytes) {
      String hexString = Integer.toHexString(0xFF & aByte);
      if (hexString.length() < 2) {
        hexString = "0" + hexString;
      }
      builder.append(hexString);
    }
    return builder.toString();
  }

  private static class SchemaFetcher implements Callable<Boolean> {
    private org.apache.avro.Schema _schema;
    private URL url;

    SchemaFetcher(URL url) {
      this.url = url;
    }

    @Override
    public Boolean call() throws Exception {
      try {
        BufferedReader reader = null;

        reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
        StringBuilder queryResp = new StringBuilder();
        for (String respLine; (respLine = reader.readLine()) != null; ) {
          queryResp.append(respLine);
        }
        _schema = org.apache.avro.Schema.parse(queryResp.toString());

        return Boolean.TRUE;
      } catch (Exception e) {
        LOGGER.warn("Caught exception while fetching schema", e);
        return Boolean.FALSE;
      }
    }

    public org.apache.avro.Schema getSchema() {
      return _schema;
    }
  }

  private org.apache.avro.Schema fetchSchema(URL url) throws Exception {
    SchemaFetcher schemaFetcher = new SchemaFetcher(url);

    boolean successful = RetryPolicies.exponentialBackoffRetryPolicy(MAXIMUM_SCHEMA_FETCH_RETRY_COUNT,
        MINIMUM_SCHEMA_FETCH_RETRY_TIME_MILLIS, SCHEMA_FETCH_RETRY_EXPONENTIAL_BACKOFF_FACTOR).attempt(schemaFetcher);

    if (successful) {
      return schemaFetcher.getSchema();
    } else {
      throw new RuntimeException(
          "Failed to fetch schema from " + url + " after " + MAXIMUM_SCHEMA_FETCH_RETRY_COUNT + "retries");
    }
  }
}
