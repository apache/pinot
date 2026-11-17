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
package org.apache.pinot.spi.stream;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class StreamConfigTest {
  private static final String JAAS_KEY = "sasl.jaas.config";
  private static final String JAAS_VALUE = "org.apache.kafka.common.security.plain.PlainLoginModule required "
      + "username=\"exampleUser\" password=\"examplePass\";";

  private static Map<String, String> baseConfigMap() {
    Map<String, String> map = new HashMap<>();
    map.put("streamType", "kafka");
    map.put("stream.kafka.topic.name", "myTopic");
    map.put("stream.kafka.decoder.class.name", "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder");
    return map;
  }

  @Test
  public void testToStringMasksSaslJaasConfig() {
    Map<String, String> map = baseConfigMap();
    map.put(JAAS_KEY, JAAS_VALUE);

    String str = new StreamConfig("myTable_REALTIME", map).toString();

    assertFalse(str.contains("exampleUser"), str);
    assertFalse(str.contains("examplePass"), str);
    assertFalse(str.contains("PlainLoginModule"), str);
    assertTrue(str.contains("sasl.jaas.config=*****"), str);
  }

  @Test
  public void testToStringMasksSaslJaasConfigInAllQuotingForms() {
    String[] jaasConfigs = {
        "required username='exampleUser' password='examplePass';",
        "required username=exampleUser password=examplePass;",
        "required username=\"exampleUser\" password=\"prefix\\\"examplePass-tail\";"
    };
    for (String jaasConfig : jaasConfigs) {
      Map<String, String> map = baseConfigMap();
      map.put(JAAS_KEY, jaasConfig);

      String str = new StreamConfig("myTable_REALTIME", map).toString();

      assertFalse(str.contains("exampleUser"), str);
      assertFalse(str.contains("examplePass"), str);
      assertTrue(str.contains("sasl.jaas.config=*****"), str);
    }
  }

  @Test
  public void testToStringMasksSensitiveKeys() {
    Map<String, String> map = baseConfigMap();
    map.put("ssl.keystore.password", "keystoreSecret");
    map.put("ssl.truststore.password", "truststoreSecret");
    map.put("some.api.token", "tokenSecret");

    String str = new StreamConfig("myTable_REALTIME", map).toString();

    assertFalse(str.contains("keystoreSecret"), str);
    assertFalse(str.contains("truststoreSecret"), str);
    assertFalse(str.contains("tokenSecret"), str);
    assertTrue(str.contains("ssl.keystore.password=*****"), str);
  }

  @Test
  public void testToStringRedactsDecoderProperties() {
    Map<String, String> map = baseConfigMap();
    map.put("stream.kafka.decoder.prop.sasl.jaas.config", JAAS_VALUE);
    map.put("stream.kafka.decoder.prop.schema.registry.password", "registrySecret");

    String str = new StreamConfig("myTable_REALTIME", map).toString();

    assertFalse(str.contains("exampleUser"), str);
    assertFalse(str.contains("examplePass"), str);
    assertFalse(str.contains("registrySecret"), str);
    assertTrue(str.contains("sasl.jaas.config=*****"), str);
    assertTrue(str.contains("schema.registry.password=*****"), str);
  }

  @Test
  public void testToStringKeepsNonSensitiveValues() {
    Map<String, String> map = baseConfigMap();
    map.put("bootstrap.servers", "localhost:9092");

    String str = new StreamConfig("myTable_REALTIME", map).toString();

    assertTrue(str.contains("_topicName='myTopic'"), str);
    assertTrue(str.contains("bootstrap.servers=localhost:9092"), str);
  }

  @Test
  public void testRedactionDoesNotMutateUnderlyingConfig() {
    Map<String, String> map = baseConfigMap();
    map.put(JAAS_KEY, JAAS_VALUE);
    map.put("ssl.keystore.password", "keystoreSecret");
    map.put("stream.kafka.decoder.prop.sasl.jaas.config", JAAS_VALUE);
    StreamConfig streamConfig = new StreamConfig("myTable_REALTIME", map);

    streamConfig.toString();

    // The real values must stay intact, because they are handed to the stream consumer clients
    assertEquals(streamConfig.getStreamConfigsMap().get(JAAS_KEY), JAAS_VALUE);
    assertEquals(streamConfig.getStreamConfigsMap().get("ssl.keystore.password"), "keystoreSecret");
    assertEquals(streamConfig.getDecoderProperties().get(JAAS_KEY), JAAS_VALUE);
  }
}
