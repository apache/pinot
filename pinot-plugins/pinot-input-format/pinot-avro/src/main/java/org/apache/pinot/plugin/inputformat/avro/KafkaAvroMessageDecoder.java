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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of StreamMessageDecoder to read avro from a Kafka stream
 * NOTE: Do not use schema in the implementation, as schema will be removed from the params
 */
@NotThreadSafe
public class KafkaAvroMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroMessageDecoder.class);

  private static final String SCHEMA_REGISTRY_REST_URL = "schema.registry.rest.url";
  private static final String SCHEMA_REGISTRY_SCHEMA_NAME = "schema.registry.schema.name";
  private org.apache.avro.Schema _defaultAvroSchema;
  private MD5AvroSchemaMap _md5ToAvroSchemaMap;

  // A global cache for schemas across all threads.
  private static final Map<String, org.apache.avro.Schema> GLOBAL_SCHEMA_CACHE = new HashMap<>();
  // Suffix for getting the latest schema
  private static final String LATEST = "-latest";

  // Reusable byte[] to read MD5 from payload. This is OK as this class is used only by a single thread.
  private final byte[] _reusableMD5Bytes = new byte[SCHEMA_HASH_LENGTH];

  private DecoderFactory _decoderFactory;
  private RecordExtractor<GenericData.Record> _avroRecordExtractor;

  private static final int MAGIC_BYTE_LENGTH = 1;
  private static final int SCHEMA_HASH_LENGTH = 16;
  private static final int HEADER_LENGTH = MAGIC_BYTE_LENGTH + SCHEMA_HASH_LENGTH;

  private static final int SCHEMA_HASH_START_OFFSET = MAGIC_BYTE_LENGTH;

  private static final int MAXIMUM_SCHEMA_FETCH_RETRY_COUNT = 5;
  private static final int MINIMUM_SCHEMA_FETCH_RETRY_TIME_MILLIS = 500;
  private static final float SCHEMA_FETCH_RETRY_EXPONENTIAL_BACKOFF_FACTOR = 2.0f;

  private String[] _schemaRegistryUrls;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    _schemaRegistryUrls = parseSchemaRegistryUrls(props.get(SCHEMA_REGISTRY_REST_URL));

    String avroSchemaName = topicName;
    if (props.containsKey(SCHEMA_REGISTRY_SCHEMA_NAME) && props.get(SCHEMA_REGISTRY_SCHEMA_NAME) != null && !props
        .get(SCHEMA_REGISTRY_SCHEMA_NAME).isEmpty()) {
      avroSchemaName = props.get(SCHEMA_REGISTRY_SCHEMA_NAME);
    }
    // With the logic below, we may not set defaultAvroSchema to be the latest one everytime.
    // The schema is fetched once when the machine starts. Until the next restart. the latest schema is
    // not fetched.
    // But then we always pay attention to the exact MD5 hash and attempt to fetch the schema for that particular hash
    // before decoding an incoming kafka event. We use defaultAvroSchema only if the fetch for the particular MD5 fails,
    // but then we will retry that fetch on every event in case of failure.
    synchronized (GLOBAL_SCHEMA_CACHE) {
      final String hashKey = avroSchemaName + LATEST;
      _defaultAvroSchema = GLOBAL_SCHEMA_CACHE.get(hashKey);
      if (_defaultAvroSchema == null) {
        _defaultAvroSchema = fetchSchema("/latest_with_type=" + avroSchemaName);
        GLOBAL_SCHEMA_CACHE.put(hashKey, _defaultAvroSchema);
        LOGGER.info("Populated schema cache with schema for {}", hashKey);
      }
    }
    String recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
    // Backward compatibility to support Avro by default
    if (recordExtractorClass == null) {
      recordExtractorClass = AvroRecordExtractor.class.getName();
    }
    _avroRecordExtractor = PluginManager.get().createInstance(recordExtractorClass);
    _avroRecordExtractor.init(fieldsToRead, null);
    _decoderFactory = new DecoderFactory();
    _md5ToAvroSchemaMap = new MD5AvroSchemaMap();
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

    System.arraycopy(payload, SCHEMA_HASH_START_OFFSET + offset, _reusableMD5Bytes, 0, SCHEMA_HASH_LENGTH);

    boolean schemaUpdateFailed = false;
    org.apache.avro.Schema schema = _md5ToAvroSchemaMap.getSchema(_reusableMD5Bytes);
    if (schema == null) {
      // We will get here for the first row consumed in the segment, and every row that has a schema ID that is
      // not yet in md5ToAvroSchemaMap.
      synchronized (GLOBAL_SCHEMA_CACHE) {
        final String hashKey = hex(_reusableMD5Bytes);
        schema = GLOBAL_SCHEMA_CACHE.get(hashKey);
        if (schema == null) {
          // We will get here only if no partition of the table has populated the global schema cache.
          // In that case, one of the consumers will fetch the schema and populate the cache, and the others
          // should find it in the cache and po
          final String schemaUri = "/id=" + hex(_reusableMD5Bytes);
          try {
            schema = fetchSchema(schemaUri);
            GLOBAL_SCHEMA_CACHE.put(hashKey, schema);
            _md5ToAvroSchemaMap.addSchema(_reusableMD5Bytes, schema);
          } catch (Exception e) {
            schema = _defaultAvroSchema;
            LOGGER
                .error("Error fetching schema using url {}. Attempting to continue with previous schema", schemaUri, e);
            schemaUpdateFailed = true;
          }
        } else {
          LOGGER.info("Found schema for {} in cache", hashKey);
          _md5ToAvroSchemaMap.addSchema(_reusableMD5Bytes, schema);
        }
      }
    }
    DatumReader<Record> reader = new GenericDatumReader<Record>(schema);
    try {
      GenericData.Record avroRecord = reader.read(null,
          _decoderFactory.createBinaryDecoder(payload, HEADER_LENGTH + offset, length - HEADER_LENGTH, null));
      return _avroRecordExtractor.extract(avroRecord, destination);
    } catch (IOException e) {
      LOGGER.error("Caught exception while reading message using schema {}{}",
          (schema == null ? "null" : schema.getName()),
          (schemaUpdateFailed ? "(possibly due to schema update failure)" : ""), e);
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
    private URL _url;
    private boolean _isSuccessful = false;

    SchemaFetcher(URL url) {
      _url = url;
    }

    @Override
    public Boolean call()
        throws Exception {
      try {
        URLConnection conn = _url.openConnection();
        conn.setConnectTimeout(15000);
        conn.setReadTimeout(15000);
        LOGGER.info("Fetching schema using url {}", _url.toString());

        StringBuilder queryResp = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            queryResp.append(line);
          }
        }

        _schema = org.apache.avro.Schema.parse(queryResp.toString());

        LOGGER.info("Schema fetch succeeded on url {}", _url.toString());
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

  private org.apache.avro.Schema fetchSchema(String reference)
      throws Exception {
    SchemaFetcher schemaFetcher = new SchemaFetcher(makeRandomUrl(reference));
    RetryPolicies
        .exponentialBackoffRetryPolicy(MAXIMUM_SCHEMA_FETCH_RETRY_COUNT, MINIMUM_SCHEMA_FETCH_RETRY_TIME_MILLIS,
            SCHEMA_FETCH_RETRY_EXPONENTIAL_BACKOFF_FACTOR).attempt(schemaFetcher);
    return schemaFetcher.getSchema();
  }

  /**
   * Private class for encapsulating MD5 to Avro schema mapping.
   * <ul>
   *   <li> Maintains two lists, one for md5s and another for schema. </li>
   *   <li> MD5 at index i in the MD5 list, corresponds to Schema at index i in the schema list. </li>
   * </ul>
   */
  private static class MD5AvroSchemaMap {
    private List<byte[]> _md5s;
    private List<org.apache.avro.Schema> _schemas;

    /**
     * Constructor for the class.
     */
    private MD5AvroSchemaMap() {
      _md5s = new ArrayList<>();
      _schemas = new ArrayList<>();
    }

    /**
     * Returns the Avro schema corresponding to the given MD5.
     *
     * @param md5ForSchema MD5 for which to get the avro schema.
     * @return Avro schema for the given MD5.
     */
    private org.apache.avro.Schema getSchema(byte[] md5ForSchema) {
      for (int i = 0; i < _md5s.size(); i++) {
        if (Arrays.equals(_md5s.get(i), md5ForSchema)) {
          return _schemas.get(i);
        }
      }
      return null;
    }

    /**
     * Adds mapping between MD5 and Avro schema.
     * Caller to ensure that addSchema is called only once per MD5-Schema pair.
     *
     * @param md5 MD5 for the Schema
     * @param schema Avro Schema
     */
    private void addSchema(byte[] md5, org.apache.avro.Schema schema) {
      _md5s.add(Arrays.copyOf(md5, md5.length));
      _schemas.add(schema);
    }
  }

  protected URL makeRandomUrl(String reference)
      throws MalformedURLException {
    Random rand = new Random();
    int randomInteger = rand.nextInt(_schemaRegistryUrls.length);
    return new URL(_schemaRegistryUrls[randomInteger] + reference);
  }

  protected String[] parseSchemaRegistryUrls(String schemaConfig) {
    return schemaConfig.split(",");
  }
}
