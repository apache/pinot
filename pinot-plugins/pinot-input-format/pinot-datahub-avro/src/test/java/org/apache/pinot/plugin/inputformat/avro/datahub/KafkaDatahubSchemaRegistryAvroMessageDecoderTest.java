/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.avro.datahub;

import static org.apache.pinot.plugin.inputformat.avro.datahub.KafkaDatahubSchemaRegistryAvroMessageDecoder.MAGIC_BYTE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Decodes avro messages with datahub schema registry.
 * NOTE: Do not use schema in the implementation, as schema will be removed from the params
 */
public class KafkaDatahubSchemaRegistryAvroMessageDecoderTest {

  KafkaDatahubSchemaRegistryAvroMessageDecoder avroMessageDecoder;

  @BeforeClass
  public void setUp() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.cluster", "b-3.devappskafka.hfnqqr.c13.kafka.us-west-2.amazonaws.com:9096,b-2.devappskafka.hfnqqr.c13.kafka.us-west-2.amazonaws.com:9096,b-1.devappskafka.hfnqqr.c13.kafka.us-west-2.amazonaws.com:9096");
    props.put("schema.registry.schemaVersion", "v1");
    props.put("schema.registry.machineAccountName", "insights.admin");
    props.put("schema.registry.machineAccountOrgId", "6078fba4-49d9-4291-9f7b-80116aab6974");
    props.put("schema.registry.machineClientId", "xxx");
    props.put("schema.registry.machineClientSecret", "xxx");
    props.put("schema.registry.machineAccountPassword", "xxx");
    avroMessageDecoder = new KafkaDatahubSchemaRegistryAvroMessageDecoder();
    avroMessageDecoder.init(props, Collections.singleton("agentName"), "wxcc_agent_event");
  }

  @Test
  public void testDecode() throws IOException {
    byte[] avroPayload = getTestPayload();
    GenericRow row = new GenericRow();
    avroMessageDecoder.decode(avroPayload, row);
    Assert.assertEquals(row.getValue("agentName"), "YiHe");
  }

  private byte[] getTestPayload() throws IOException {
    String schemaVersion = "v10";
    Schema schema = getAvroSchema();
    GenericDatumWriter<GenericRecord> datumWriter
        = new GenericDatumWriter<>(schema, new GenericData(Thread.currentThread().getContextClassLoader()));
    ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(arrayOutputStream, null);

    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("orgId", "YiHe");
    avroRecord.put("intervalStartTime", 1722402679252L);
    avroRecord.put("eventTime", 1722402679252L);
    avroRecord.put("agentName", "YiHe");
    avroRecord.put("agentId", "YiHe");
    avroRecord.put("siteId", "YiHe");
    avroRecord.put("siteName", "YiHe");
    avroRecord.put("teamId", "YiHe");
    avroRecord.put("teamName", "YiHe");

    arrayOutputStream.write(MAGIC_BYTE);
    byte[] versionBytes = schemaVersion.getBytes(StandardCharsets.UTF_8);
    arrayOutputStream.write(versionBytes.length);
    arrayOutputStream.write(ByteBuffer.allocate(versionBytes.length).put(versionBytes).array());
    datumWriter.write(avroRecord, encoder);
    encoder.flush();
    byte[] bytes = arrayOutputStream.toByteArray();
    arrayOutputStream.reset();
    return bytes;
  }


  private static Schema getAvroSchema() {
    String avroSchemaStr = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"realtimeAgentGlobalStatistics\",\n"
        + "  \"namespace\": \"wxcc\",\n"
        + "  \"doc\": \"This Schema defines realtime agent global statistics metrics\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"tenantId\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"string\"\n"
        + "      ],\n"
        + "      \"default\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"agentPhone\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"string\"\n"
        + "      ],\n"
        + "      \"default\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"orgId\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"agentId\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"agentName\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"intervalStartTime\",\n"
        + "      \"type\": {\n"
        + "        \"type\": \"long\",\n"
        + "        \"logicalType\": \"timestamp-millis\"\n"
        + "      }\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"totalIdleCount\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"int\"\n"
        + "      ],\n"
        + "      \"default\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"totalIdleDuration\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"long\"\n"
        + "      ],\n"
        + "      \"default\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"totalAvailableCount\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"int\"\n"
        + "      ],\n"
        + "      \"default\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"totalAvailableDuration\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"long\"\n"
        + "      ],\n"
        + "      \"default\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"totalLogin\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"int\"\n"
        + "      ],\n"
        + "      \"default\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"eventTime\",\n"
        + "      \"type\": {\n"
        + "        \"type\": \"long\",\n"
        + "        \"logicalType\": \"timestamp-millis\"\n"
        + "      }\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"siteId\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"siteName\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"teamId\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"teamName\",\n"
        + "      \"type\": \"string\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    Schema.Parser parser = new Schema.Parser();
    return parser.parse(avroSchemaStr);
  }


}
