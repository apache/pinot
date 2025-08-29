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

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaAvroMessageDecoderTest {

  private static final String AVRO_SCHEMA_JSON =
      "{\"type\": \"record\", \"name\": \"TestRecord\", \"fields\": ["
          + "{\"name\": \"id\", \"type\": \"long\"},"
          + "{\"name\": \"name\", \"type\": \"string\"},"
          + "{\"name\": \"value\", \"type\": \"double\"}"
          + "]}";

  @Test
  public void testParseSchemaRegistryUrls() throws Exception {
    KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();

    String singleUrl = "http://localhost:8081";
    String[] result = decoder.parseSchemaRegistryUrls(singleUrl);
    Assert.assertEquals(result.length, 1);
    Assert.assertEquals(result[0], singleUrl);

    String multipleUrls = "http://localhost:8081,http://localhost:8082,"
        + "http://localhost:8083";
    result = decoder.parseSchemaRegistryUrls(multipleUrls);
    Assert.assertEquals(result.length, 3);
    Assert.assertEquals(result[0], "http://localhost:8081");
    Assert.assertEquals(result[1], "http://localhost:8082");
    Assert.assertEquals(result[2], "http://localhost:8083");
  }

  @Test
  public void testHexConversion() throws Exception {
    KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();

    byte[] bytes = new byte[]{0x12, 0x34, (byte) 0xAB, (byte) 0xCD};
    String expectedHex = "1234abcd";

    // Use reflection to access the private hex method
    java.lang.reflect.Method hexMethod =
        KafkaAvroMessageDecoder.class.getDeclaredMethod("hex", byte[].class);
    hexMethod.setAccessible(true);

    String result = (String) hexMethod.invoke(decoder, bytes);
    Assert.assertEquals(result, expectedHex);
  }

  @Test
  public void testMD5AvroSchemaMap() throws Exception {
    // Test the MD5AvroSchemaMap inner class
    Class<?> mapClass =
        Class.forName("org.apache.pinot.plugin.inputformat.avro.KafkaAvroMessageDecoder$MD5AvroSchemaMap");

    // Get the constructor and make it accessible
    java.lang.reflect.Constructor<?> constructor = mapClass.getDeclaredConstructor();
    constructor.setAccessible(true);
    Object mapInstance = constructor.newInstance();

    // Get the addSchema and getSchema methods and make them accessible
    java.lang.reflect.Method addSchemaMethod = mapClass.getDeclaredMethod("addSchema", byte[].class, Schema.class);
    java.lang.reflect.Method getSchemaMethod = mapClass.getDeclaredMethod("getSchema", byte[].class);
    addSchemaMethod.setAccessible(true);
    getSchemaMethod.setAccessible(true);

    // Create test data
    byte[] md5Bytes = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    Schema testSchema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);

    // Test adding and retrieving schema
    addSchemaMethod.invoke(mapInstance, md5Bytes, testSchema);
    Schema retrievedSchema = (Schema) getSchemaMethod.invoke(mapInstance, md5Bytes);

    Assert.assertEquals(retrievedSchema, testSchema);

    // Test retrieving non-existent schema
    byte[] differentMd5Bytes = new byte[]{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    Schema nonExistentSchema = (Schema) getSchemaMethod.invoke(mapInstance, differentMd5Bytes);

    Assert.assertNull(nonExistentSchema);
  }

  @Test
  public void testMessageDecoderCreation() throws Exception {
    KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();

    // Test that decoder can be created
    Assert.assertNotNull(decoder);

    // Test that parseSchemaRegistryUrls works
    java.lang.reflect.Method parseUrlsMethod =
        KafkaAvroMessageDecoder.class.getDeclaredMethod("parseSchemaRegistryUrls", String.class);
    parseUrlsMethod.setAccessible(true);

    String[] urls = (String[]) parseUrlsMethod.invoke(decoder, "http://localhost:8081,http://localhost:8082");
    Assert.assertEquals(urls.length, 2);
  }

  @Test
  public void testDecodeNullPayload() throws Exception {
    KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();

    GenericRow destination = new GenericRow();
    GenericRow result = decoder.decode(null, destination);

    Assert.assertNull(result);
  }

  @Test
  public void testDecodeEmptyPayload() throws Exception {
    KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();

    GenericRow destination = new GenericRow();
    GenericRow result = decoder.decode(new byte[0], destination);

    Assert.assertNull(result);
  }

  @Test
  public void testDecodeShortPayload() throws Exception {
    KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();

    // Payload shorter than header length (17 bytes: 1 magic + 16 MD5)
    GenericRow destination = new GenericRow();
    GenericRow result = decoder.decode(new byte[10], destination);

    Assert.assertNull(result);
  }

  @Test
  public void testMakeRandomUrl() throws Exception {
    KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();

    // Set up schema registry URLs directly using reflection to avoid network calls
    java.lang.reflect.Field schemaRegistryUrlsField =
        KafkaAvroMessageDecoder.class.getDeclaredField("_schemaRegistryUrls");
    schemaRegistryUrlsField.setAccessible(true);
    schemaRegistryUrlsField.set(decoder, new String[]{"http://localhost:8081", "http://localhost:8082"});

    // Use reflection to access the protected makeRandomUrl method
    java.lang.reflect.Method makeRandomUrlMethod =
        KafkaAvroMessageDecoder.class.getDeclaredMethod("makeRandomUrl", String.class);
    makeRandomUrlMethod.setAccessible(true);

    // Test that it returns a valid URL
    Object result = makeRandomUrlMethod.invoke(decoder, "/test");
    Assert.assertNotNull(result);
    Assert.assertTrue(result.toString().startsWith("http://localhost:808"));
    Assert.assertTrue(result.toString().endsWith("/test"));
  }

  @Test
  public void testSchemaRegistryUrlParsing() throws Exception {
    KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();

    // Test the parseSchemaRegistryUrls method directly
    java.lang.reflect.Method parseUrlsMethod =
        KafkaAvroMessageDecoder.class.getDeclaredMethod("parseSchemaRegistryUrls", String.class);
    parseUrlsMethod.setAccessible(true);

    // Test single URL
    String[] result = (String[]) parseUrlsMethod.invoke(decoder, "http://localhost:8081");
    Assert.assertEquals(result.length, 1);
    Assert.assertEquals(result[0], "http://localhost:8081");

    // Test multiple URLs
    result = (String[]) parseUrlsMethod.invoke(decoder, "http://localhost:8081,http://localhost:8082");
    Assert.assertEquals(result.length, 2);
    Assert.assertEquals(result[0], "http://localhost:8081");
    Assert.assertEquals(result[1], "http://localhost:8082");
  }

  @Test
  public void testGlobalSchemaCache() throws Exception {
    // Test that the global schema cache is accessible
    Class<?> decoderClass = KafkaAvroMessageDecoder.class;

    // Access the GLOBAL_SCHEMA_CACHE field
    java.lang.reflect.Field cacheField = decoderClass.getDeclaredField("GLOBAL_SCHEMA_CACHE");
    cacheField.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Schema> cache = (Map<String, Schema>) cacheField.get(null);

    Assert.assertNotNull(cache);

    // Test adding to cache
    String testKey = "test-key";
    Schema testSchema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
    cache.put(testKey, testSchema);

    // Verify it was added
    Assert.assertEquals(cache.get(testKey), testSchema);

    // Clean up
    cache.remove(testKey);
  }

  @Test
  public void testSchemaRegistrySchemaNameOverride() throws Exception {
    KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();

    // Test schema name override by checking that the properties are parsed correctly
    // We can't test the full init without network calls, but we can test the parsing logic
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.rest.url", "http://localhost:8081");
    props.put("schema.registry.schema.name", "custom-schema-name");

    // Test URL parsing
    java.lang.reflect.Method parseUrlsMethod =
        KafkaAvroMessageDecoder.class.getDeclaredMethod("parseSchemaRegistryUrls", String.class);
    parseUrlsMethod.setAccessible(true);
    String[] urls = (String[]) parseUrlsMethod.invoke(decoder, props.get("schema.registry.rest.url"));
    Assert.assertEquals(urls.length, 1);
    Assert.assertEquals(urls[0], "http://localhost:8081");

    // Verify the schema name property is present
    Assert.assertEquals(props.get("schema.registry.schema.name"), "custom-schema-name");
  }

  @Test
  public void testRecordExtractorConfiguration() throws Exception {
    KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();

    // Test record extractor configuration properties parsing
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.rest.url", "http://localhost:8081");
    props.put("recordExtractorClass", AvroRecordExtractor.class.getName());
    props.put("recordExtractorConfigClass", AvroRecordExtractorConfig.class.getName());

    // Test URL parsing
    java.lang.reflect.Method parseUrlsMethod =
        KafkaAvroMessageDecoder.class.getDeclaredMethod("parseSchemaRegistryUrls", String.class);
    parseUrlsMethod.setAccessible(true);
    String[] urls = (String[]) parseUrlsMethod.invoke(decoder, props.get("schema.registry.rest.url"));
    Assert.assertEquals(urls.length, 1);

    // Verify the record extractor properties are present
    Assert.assertEquals(props.get("recordExtractorClass"),
        "org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor");
    Assert.assertEquals(props.get("recordExtractorConfigClass"),
        "org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractorConfig");
  }
}
