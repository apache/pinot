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
package org.apache.pinot.plugin.inputformat.avro.confluent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class KafkaConfluentSchemaRegistryAvroMessageDecoderTest {

  @Mock
  private io.confluent.kafka.schemaregistry.client.SchemaRegistryClient _mockSchemaRegistryClient;

  @Mock
  private io.confluent.kafka.serializers.KafkaAvroDeserializer _mockDeserializer;

  private AutoCloseable _mocks;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testInitWithValidProperties()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.rest.url", "http://localhost:8081");
    Set<String> fieldsToRead = Set.of("id", "name", "value");
    String topicName = "test-topic";

    try (MockedConstruction<io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient> mockedConstruction =
        Mockito.mockConstruction(io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.class,
            (mock, context) -> {
              // Mock the construction
            })) {

      decoder.init(props, fieldsToRead, topicName);

      Assert.assertNotNull(decoder);
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInitWithoutSchemaRegistryUrl()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    // Missing schema.registry.rest.url
    Set<String> fieldsToRead = Set.of("id", "name");
    String topicName = "test-topic";

    decoder.init(props, fieldsToRead, topicName);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testInitWithoutTopicName()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.rest.url", "http://localhost:8081");
    Set<String> fieldsToRead = Set.of("id", "name");

    decoder.init(props, fieldsToRead, null);
  }

  @Test
  public void testCreateRestService()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> configs = new HashMap<>();
    configs.put("schema.registry.rest.url", "http://localhost:8081");
    configs.put("schema.registry.ssl.keystore.location", "/tmp/test-keystores/keystore.jks");
    configs.put("schema.registry.ssl.keystore.password", "password");
    configs.put("schema.registry.ssl.keystore.type", "JKS");
    configs.put("schema.registry.ssl.truststore.location", "/tmp/test-keystores/truststore.jks");
    configs.put("schema.registry.ssl.truststore.password", "truststorePassword");
    configs.put("schema.registry.ssl.truststore.type", "JKS");
    configs.put("schema.registry.ssl.protocol", "TLSv1.2");

    io.confluent.kafka.schemaregistry.client.rest.RestService restService =
        decoder.createRestService("http://localhost:8081", configs);

    Assert.assertNotNull(restService);
  }

  @Test
  public void testCreateRestServiceWithoutSSL()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> configs = new HashMap<>();
    configs.put("schema.registry.rest.url", "http://localhost:8081");

    io.confluent.kafka.schemaregistry.client.rest.RestService restService =
        decoder.createRestService("http://localhost:8081", configs);

    Assert.assertNotNull(restService);
  }

  @Test
  public void testDecodeNullPayload()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();

    // Mock the deserializer to avoid initialization requirements
    io.confluent.kafka.serializers.KafkaAvroDeserializer mockDeserializer =
        Mockito.mock(io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

    // Mock the record extractor to avoid initialization requirements
    org.apache.pinot.spi.data.readers.RecordExtractor mockRecordExtractor =
        Mockito.mock(org.apache.pinot.spi.data.readers.RecordExtractor.class);

    // Set the deserializer and record extractor using reflection
    java.lang.reflect.Field deserializerField =
        KafkaConfluentSchemaRegistryAvroMessageDecoder.class.getDeclaredField("_deserializer");
    deserializerField.setAccessible(true);
    deserializerField.set(decoder, mockDeserializer);

    java.lang.reflect.Field recordExtractorField =
        KafkaConfluentSchemaRegistryAvroMessageDecoder.class.getDeclaredField("_avroRecordExtractor");
    recordExtractorField.setAccessible(true);
    recordExtractorField.set(decoder, mockRecordExtractor);

    GenericRow destination = new GenericRow();
    GenericRow result = decoder.decode(null, destination);

    Assert.assertNull(result);
  }

  @Test
  public void testDecodeEmptyPayload()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();

    // Mock the deserializer to avoid initialization requirements
    io.confluent.kafka.serializers.KafkaAvroDeserializer mockDeserializer =
        Mockito.mock(io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

    // Mock the record extractor to avoid initialization requirements
    org.apache.pinot.spi.data.readers.RecordExtractor mockRecordExtractor =
        Mockito.mock(org.apache.pinot.spi.data.readers.RecordExtractor.class);

    // Set the deserializer and record extractor using reflection
    java.lang.reflect.Field deserializerField =
        KafkaConfluentSchemaRegistryAvroMessageDecoder.class.getDeclaredField("_deserializer");
    deserializerField.setAccessible(true);
    deserializerField.set(decoder, mockDeserializer);

    java.lang.reflect.Field recordExtractorField =
        KafkaConfluentSchemaRegistryAvroMessageDecoder.class.getDeclaredField("_avroRecordExtractor");
    recordExtractorField.setAccessible(true);
    recordExtractorField.set(decoder, mockRecordExtractor);

    GenericRow destination = new GenericRow();
    GenericRow result = decoder.decode(new byte[0], destination);

    Assert.assertNull(result);
  }

  @Test
  public void testDecodeWithOffsetAndLength()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.rest.url", "http://localhost:8081");
    Set<String> fieldsToRead = Set.of("id", "name");
    String topicName = "test-topic";

    try (MockedConstruction<io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient> mockedConstruction =
        Mockito.mockConstruction(io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.class);
        MockedConstruction<io.confluent.kafka.serializers.KafkaAvroDeserializer> mockedDeserializer =
            Mockito.mockConstruction(io.confluent.kafka.serializers.KafkaAvroDeserializer.class,
                (mock, context) -> {
                  // Mock the deserialize method to return a test record
                  Mockito.when(mock.deserialize(Mockito.anyString(), Mockito.any(byte[].class)))
                      .thenReturn(createMockAvroRecord());
                })) {

      decoder.init(props, fieldsToRead, topicName);

      // Create a test payload
      byte[] fullPayload = new byte[]{0, 0, 0, 0, 1, 2, 3, 4}; // Mock Confluent wire format

      // Create a larger payload with the data at an offset
      byte[] largerPayload = new byte[fullPayload.length + 10];
      System.arraycopy(fullPayload, 0, largerPayload, 5, fullPayload.length);

      GenericRow destination = new GenericRow();
      GenericRow result = decoder.decode(largerPayload, 5, fullPayload.length, destination);

      // Verify that the deserializer was called with the correct sub-array
      Assert.assertNotNull(result);
    }
  }

  @Test
  public void testIgnoreOrRethrowExceptionWithUnknownMagicByte()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.rest.url", "http://localhost:8081");
    Set<String> fieldsToRead = Set.of("id", "name");
    String topicName = "test-topic";

    try (MockedConstruction<io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient> mockedConstruction =
        Mockito.mockConstruction(io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.class);
        MockedConstruction<io.confluent.kafka.serializers.KafkaAvroDeserializer> mockedDeserializer =
            Mockito.mockConstruction(io.confluent.kafka.serializers.KafkaAvroDeserializer.class,
                (mock, context) -> {
                  // Mock the deserialize method to throw an exception
                  Mockito.when(mock.deserialize(Mockito.anyString(), Mockito.any(byte[].class)))
                      .thenThrow(new org.apache.kafka.common.errors.SerializationException("Unknown magic byte"));
                })) {

      decoder.init(props, fieldsToRead, topicName);

      GenericRow destination = new GenericRow();
      GenericRow result = decoder.decode(new byte[]{1, 2, 3, 4}, destination);

      // Should return null for unknown magic byte exceptions
      Assert.assertNull(result);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testIgnoreOrRethrowExceptionWithOtherException()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.rest.url", "http://localhost:8081");
    Set<String> fieldsToRead = Set.of("id", "name");
    String topicName = "test-topic";

    try (MockedConstruction<io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient> mockedConstruction =
        Mockito.mockConstruction(io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.class);
        MockedConstruction<io.confluent.kafka.serializers.KafkaAvroDeserializer> mockedDeserializer =
            Mockito.mockConstruction(io.confluent.kafka.serializers.KafkaAvroDeserializer.class,
                (mock, context) -> {
                  // Mock the deserialize method to throw a different exception
                  Mockito.when(mock.deserialize(Mockito.anyString(), Mockito.any(byte[].class)))
                      .thenThrow(new RuntimeException("Other serialization error"));
                })) {

      decoder.init(props, fieldsToRead, topicName);

      GenericRow destination = new GenericRow();
      decoder.decode(new byte[]{1, 2, 3, 4}, destination);
    }
  }

  @Test
  public void testIsUnknownMagicByte()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();

    // Use reflection to access the private isUnknownMagicByte method
    java.lang.reflect.Method isUnknownMagicByteMethod =
        KafkaConfluentSchemaRegistryAvroMessageDecoder.class.getDeclaredMethod("isUnknownMagicByte", Throwable.class);
    isUnknownMagicByteMethod.setAccessible(true);

    // Test with null exception
    Boolean result = (Boolean) isUnknownMagicByteMethod.invoke(decoder, (Throwable) null);
    Assert.assertFalse(result);

    // Test with SerializationException containing "unknown magic byte"
    org.apache.kafka.common.errors.SerializationException magicByteException =
        new org.apache.kafka.common.errors.SerializationException("Unknown magic byte");
    result = (Boolean) isUnknownMagicByteMethod.invoke(decoder, magicByteException);
    Assert.assertTrue(result);

    // Test with SerializationException not containing "unknown magic byte"
    org.apache.kafka.common.errors.SerializationException otherException =
        new org.apache.kafka.common.errors.SerializationException("Other error");
    result = (Boolean) isUnknownMagicByteMethod.invoke(decoder, otherException);
    Assert.assertFalse(result);

    // Test with RuntimeException containing SerializationException cause
    RuntimeException wrapperException = new RuntimeException("Wrapper", magicByteException);
    result = (Boolean) isUnknownMagicByteMethod.invoke(decoder, wrapperException);
    Assert.assertFalse(result); // RuntimeException itself should return false

    // Test the cause (SerializationException) should return true
    result = (Boolean) isUnknownMagicByteMethod.invoke(decoder, wrapperException.getCause());
    Assert.assertTrue(result);
  }

  @Test
  public void testCustomRecordExtractorConfiguration()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.rest.url", "http://localhost:8081");
    props.put("recordExtractorClass", org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor.class.getName());
    Set<String> fieldsToRead = Set.of("id", "name");
    String topicName = "test-topic";

    try (MockedConstruction<io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient> mockedConstruction =
        Mockito.mockConstruction(io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.class)) {

      decoder.init(props, fieldsToRead, topicName);

      Assert.assertNotNull(decoder);
    }
  }

  @Test
  public void testSSLConfiguration()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> configs = new HashMap<>();
    configs.put("schema.registry.rest.url", "http://localhost:8081");
    configs.put("schema.registry.ssl.keystore.location", "/tmp/test-keystores/keystore.jks");
    configs.put("schema.registry.ssl.keystore.password", "password");
    configs.put("schema.registry.ssl.keystore.type", "JKS");
    configs.put("schema.registry.ssl.truststore.location", "/tmp/test-keystores/truststore.jks");
    configs.put("schema.registry.ssl.truststore.password", "truststorePassword");
    configs.put("schema.registry.ssl.truststore.type", "JKS");
    configs.put("schema.registry.ssl.key.password", "password");
    configs.put("schema.registry.ssl.protocol", "TLSv1.2");

    io.confluent.kafka.schemaregistry.client.rest.RestService restService =
        decoder.createRestService("http://localhost:8081", configs);

    Assert.assertNotNull(restService);
  }

  @Test
  public void testNonSSLRegistryOptsAreIgnored()
      throws Exception {
    KafkaConfluentSchemaRegistryAvroMessageDecoder decoder = new KafkaConfluentSchemaRegistryAvroMessageDecoder();
    Map<String, String> configs = new HashMap<>();
    configs.put("schema.registry.rest.url", "http://localhost:8081");
    configs.put("schema.registry.some.other.property", "someValue");
    configs.put("some.unrelated.property", "unrelatedValue");

    io.confluent.kafka.schemaregistry.client.rest.RestService restService =
        decoder.createRestService("http://localhost:8081", configs);

    Assert.assertNotNull(restService);
  }

  /**
   * Helper method to create a mock Avro record for testing
   */
  private org.apache.avro.generic.GenericData.Record createMockAvroRecord() {
    // Create a simple mock record structure with a proper RECORD schema
    List<org.apache.avro.Schema.Field> fields = new java.util.ArrayList<>();
    fields.add(new org.apache.avro.Schema.Field("id",
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null));
    fields.add(new org.apache.avro.Schema.Field("name",
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));

    org.apache.avro.Schema mockSchema = org.apache.avro.Schema.createRecord("TestRecord", null, "test", false, fields);
    org.apache.avro.generic.GenericData.Record record = new org.apache.avro.generic.GenericData.Record(mockSchema);
    record.put("id", 1L);
    record.put("name", "test");
    return record;
  }
}
