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
package org.apache.pinot.plugin.inputformat.protobuf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.pinot.plugin.inputformat.protobuf.kafka.schemaregistry.SchemaRegistryStarter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class KafkaConfluentSchemaRegistryProtoBufMessageDecoderTest {
  SchemaRegistryStarter.KafkaSchemaRegistryInstance _schemaRegistry;

  @AfterClass
  public void tearDown() {
    _schemaRegistry.stop();
  }

  @BeforeClass
  public void setup() {
    _schemaRegistry = SchemaRegistryStarter.startLocalInstance(9093);
  }

  @Test
  void testInit()
      throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(KafkaConfluentSchemaRegistryProtoBufMessageDecoder.SCHEMA_REGISTRY_REST_URL, _schemaRegistry.getUrl());
    KafkaConfluentSchemaRegistryProtoBufMessageDecoder decoder =
        new KafkaConfluentSchemaRegistryProtoBufMessageDecoder();
    decoder.init(props, Collections.emptySet(), "asd");
  }
}
