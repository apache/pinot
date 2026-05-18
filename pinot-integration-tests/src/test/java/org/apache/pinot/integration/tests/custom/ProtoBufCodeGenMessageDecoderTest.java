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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.common.primitives.Longs;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.plugin.inputformat.protobuf.ProtoBufCodeGenMessageDecoder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Integration test for {@link ProtoBufCodeGenMessageDecoder}.
 *
 * <p>Pushes protobuf-serialized {@code SampleRecord} messages (defined in {@code sample.proto})
 * into Kafka, configures a realtime Pinot table to decode them with {@link ProtoBufCodeGenMessageDecoder},
 * and validates that data is ingested and queryable.
 *
 * <p>Required test resources in {@code pinot-integration-tests/src/test/resources/}:
 * <ul>
 *   <li>{@code sample-samplerecord.jar} — compiled protobuf classes for {@code SampleRecord}.
 *       To regenerate: copy {@code sample.jar} from
 *       {@code pinot-plugins/pinot-input-format/pinot-protobuf/src/test/resources/}.</li>
 *   <li>{@code sample-samplerecord.desc} — binary {@code FileDescriptorSet} for constructing
 *       messages via {@link DynamicMessage} without a generated Java class.
 *       To regenerate: copy {@code sample.desc} from the same source directory.</li>
 * </ul>
 * Both resources are copies of the test resources in {@code pinot-protobuf}, renamed to avoid
 * classpath conflicts with that module's own resources when both are on the test classpath.
 *
 * <p>Thread-safety: inherits the shared-cluster pattern from {@link CustomDataQueryClusterIntegrationTest}.
 */
public class ProtoBufCodeGenMessageDecoderTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME = "ProtoBufCodeGenTest";
  private static final int NUM_RECORDS = 200;
  private static final int NUM_DISTINCT_FRIENDS = 5;
  private static final String PROTO_CLASS_NAME_VALUE =
      "org.apache.pinot.plugin.inputformat.protobuf.Sample$SampleRecord";
  private static final String PROTO_DESCRIPTOR_RESOURCE = "sample-samplerecord.desc";
  private static final String PROTO_JAR_RESOURCE = "sample-samplerecord.jar";
  private static final String PROTO_MESSAGE_TYPE = "SampleRecord";

  private static final String COL_NAME = "name";
  private static final String COL_ID = "id";
  private static final String COL_EMAIL = "email";
  private static final String COL_FRIENDS = "friends";

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addField(new DimensionFieldSpec(COL_NAME, FieldSpec.DataType.STRING, true))
        .addField(new DimensionFieldSpec(COL_EMAIL, FieldSpec.DataType.STRING, true))
        .addField(new DimensionFieldSpec(COL_FRIENDS, FieldSpec.DataType.STRING, false))
        .addField(new DateTimeFieldSpec(COL_ID, FieldSpec.DataType.INT,
            "1:SECONDS:EPOCH", "1:SECONDS"))
        .build();
  }

  @Override
  public long getCountStarResult() {
    return NUM_RECORDS;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected Map<String, String> getStreamConfigMap() {
    Map<String, String> streamConfigMap = super.getStreamConfigMap();
    String streamType = "kafka";
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        ProtoBufCodeGenMessageDecoder.class.getName());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType,
            "decoder.prop." + ProtoBufCodeGenMessageDecoder.PROTOBUF_JAR_FILE_PATH),
        getJarFileUri());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType,
            "decoder.prop." + ProtoBufCodeGenMessageDecoder.PROTO_CLASS_NAME),
        PROTO_CLASS_NAME_VALUE);
    return streamConfigMap;
  }

  @Override
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    // sampleAvroFile is not used — data is pushed as protobuf bytes, not Avro
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(getTableName())
        .setStreamConfigs(getStreamConfigs())
        .setTimeColumnName(COL_ID)
        .setNumReplicas(getNumReplicas())
        .build();
  }

  /**
   * Returns a single dummy file. The base class passes {@code avroFiles.get(0)} to
   * {@link #createRealtimeTableConfig(File)}, so the list must be non-empty. The file is not read.
   */
  @Override
  public List<File> createAvroFiles()
      throws Exception {
    File dummy = new File(_tempDir, "dummy.avro");
    dummy.createNewFile();
    return List.of(dummy);
  }

  /**
   * Pushes {@link #NUM_RECORDS} protobuf-encoded {@code SampleRecord} messages to Kafka.
   *
   * <p>Uses {@link DynamicMessage} with the pre-compiled {@code sample-samplerecord.desc} descriptor
   * so no generated Java sources are needed at compile time.
   *
   * <p>Each record {@code i} has:
   * <ul>
   *   <li>{@code id = i} (also the time column, values 0–{@link #NUM_RECORDS})</li>
   *   <li>{@code name = "name-i"}</li>
   *   <li>{@code email = "user-i@example.com"}</li>
   *   <li>{@code friends = ["friend-{i % NUM_DISTINCT_FRIENDS}"]}</li>
   * </ul>
   */
  @Override
  protected void pushDataIntoKafka(List<File> dataFiles)
      throws Exception {
    Descriptors.Descriptor descriptor = loadSampleRecordDescriptor();
    Descriptors.FieldDescriptor nameField = descriptor.findFieldByName(COL_NAME);
    Descriptors.FieldDescriptor idField = descriptor.findFieldByName(COL_ID);
    Descriptors.FieldDescriptor emailField = descriptor.findFieldByName(COL_EMAIL);
    Descriptors.FieldDescriptor friendsField = descriptor.findFieldByName(COL_FRIENDS);

    try (StreamDataProducer producer =
        ClusterIntegrationTestUtils.getKafkaProducer(getSharedKafkaBrokerList())) {
      for (int i = 0; i < NUM_RECORDS; i++) {
        DynamicMessage record = DynamicMessage.newBuilder(descriptor)
            .setField(nameField, "name-" + i)
            .setField(idField, i)
            .setField(emailField, "user" + i + "@example.com")
            .addRepeatedField(friendsField, "friend-" + (i % NUM_DISTINCT_FRIENDS))
            .build();
        producer.produce(getKafkaTopic(), Longs.toByteArray(i), record.toByteArray());
      }
    }
  }

  /**
   * Verifies that all pushed records are ingested: {@code COUNT(*) = NUM_RECORDS}.
   */
  @Test
  public void testCountStar()
      throws Exception {
    JsonNode response = postQuery("SELECT COUNT(*) FROM " + getTableName());
    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_RECORDS);
  }

  /**
   * Verifies scalar field decoding by selecting a specific record by {@code id}.
   */
  @Test
  public void testSelectById()
      throws Exception {
    int targetId = 42;
    JsonNode response = postQuery(
        String.format("SELECT %s, %s FROM %s WHERE %s = %d", COL_NAME, COL_EMAIL, getTableName(), COL_ID, targetId));
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 1, "Expected exactly one row for id=" + targetId);
    assertEquals(rows.get(0).get(0).asText(), "name-" + targetId);
    assertEquals(rows.get(0).get(1).asText(), "user" + targetId + "@example.com");
  }

  /**
   * Verifies that {@code repeated string friends} is decoded as a multi-value STRING column.
   * Each record has one friend: {@code "friend-{id % NUM_DISTINCT_FRIENDS}"},
   * so each distinct friend value should match {@code NUM_RECORDS / NUM_DISTINCT_FRIENDS} rows.
   */
  @Test
  public void testMultiValueFriendsCount()
      throws Exception {
    long expectedPerFriend = NUM_RECORDS / NUM_DISTINCT_FRIENDS;
    for (int f = 0; f < NUM_DISTINCT_FRIENDS; f++) {
      JsonNode response = postQuery(
          String.format("SELECT COUNT(*) FROM %s WHERE %s = 'friend-%d'", getTableName(), COL_FRIENDS, f));
      long count = response.get("resultTable").get("rows").get(0).get(0).asLong();
      assertEquals(count, expectedPerFriend,
          "Unexpected row count for friend-" + f + ": " + count + " (expected " + expectedPerFriend + ")");
    }
  }

  /**
   * Verifies there are exactly {@link #NUM_DISTINCT_FRIENDS} distinct values in the {@code friends} column.
   */
  @Test
  public void testDistinctFriendCount()
      throws Exception {
    JsonNode response = postQuery(
        String.format("SELECT COUNT(DISTINCT %s) FROM %s", COL_FRIENDS, getTableName()));
    long count = response.get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(count, NUM_DISTINCT_FRIENDS,
        "Expected " + NUM_DISTINCT_FRIENDS + " distinct friend values, got " + count);
  }

  /**
   * Verifies that the {@code email} field is decoded and non-null for all records.
   */
  @Test
  public void testEmailFieldNotNull()
      throws Exception {
    JsonNode response = postQuery(
        String.format("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL", getTableName(), COL_EMAIL));
    long count = response.get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(count, NUM_RECORDS, "All records should have a non-null email");
  }

  // ---- Helpers ----

  /**
   * Loads the {@code SampleRecord} {@link Descriptors.Descriptor} from the binary descriptor file
   * on the test classpath. Fails with a clear assertion message if the resource or message type
   * is not found.
   */
  private static Descriptors.Descriptor loadSampleRecordDescriptor()
      throws Exception {
    try (InputStream is = ProtoBufCodeGenMessageDecoderTest.class.getClassLoader()
        .getResourceAsStream(PROTO_DESCRIPTOR_RESOURCE)) {
      assertNotNull(is, PROTO_DESCRIPTOR_RESOURCE + " not found on test classpath");
      DynamicSchema schema = DynamicSchema.parseFrom(is);
      Descriptors.Descriptor descriptor = schema.getMessageDescriptor(PROTO_MESSAGE_TYPE);
      assertNotNull(descriptor,
          "Message type '" + PROTO_MESSAGE_TYPE + "' not found in " + PROTO_DESCRIPTOR_RESOURCE
              + ". Verify the descriptor was generated correctly from sample.proto.");
      return descriptor;
    }
  }

  private static String getJarFileUri() {
    URL jarUrl = ProtoBufCodeGenMessageDecoderTest.class.getClassLoader()
        .getResource(PROTO_JAR_RESOURCE);
    try {
      return Objects.requireNonNull(jarUrl, PROTO_JAR_RESOURCE + " not found on test classpath")
          .toURI().toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to resolve JAR URI for " + PROTO_JAR_RESOURCE, e);
    }
  }
}
