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
package org.apache.pinot.minion.taskfactory;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.utils.Obfuscator;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TaskFactoryRegistryTest {

  @Test
  public void testObfuscatorMasksSensitiveConfigs() {
    // Test that the Obfuscator properly masks sensitive configuration values
    Map<String, String> configs = new HashMap<>();
    configs.put("tableName", "myTable");
    configs.put("authToken", "Basic YWRtaW46dmVyeXNlY3JldA");
    configs.put("password", "verysecret");
    configs.put("secretKey", "mySecretKey123");
    configs.put("apiKey", "sk-1234567890abcdef");
    configs.put("normalConfig", "normalValue");

    String obfuscatedJson = Obfuscator.DEFAULT.toJsonString(configs);

    // Verify that sensitive values are masked
    Assert.assertTrue(obfuscatedJson.contains("tableName"));
    Assert.assertTrue(obfuscatedJson.contains("normalConfig"));
    Assert.assertTrue(obfuscatedJson.contains("normalValue"));

    // Verify that sensitive values are masked with "*****"
    Assert.assertTrue(obfuscatedJson.contains("\"authToken\":\"*****\""));
    Assert.assertTrue(obfuscatedJson.contains("\"password\":\"*****\""));
    Assert.assertTrue(obfuscatedJson.contains("\"secretKey\":\"*****\""));
    Assert.assertTrue(obfuscatedJson.contains("\"apiKey\":\"*****\""));

    // Verify that the original sensitive values are not present
    Assert.assertFalse(obfuscatedJson.contains("Basic YWRtaW46dmVyeXNlY3JldA"));
    Assert.assertFalse(obfuscatedJson.contains("verysecret"));
    Assert.assertFalse(obfuscatedJson.contains("mySecretKey123"));
    Assert.assertFalse(obfuscatedJson.contains("sk-1234567890abcdef"));
  }

  @Test
  public void testObfuscatorHandlesNullAndEmptyValues() {
    Map<String, String> configs = new HashMap<>();
    configs.put("authToken", null);
    configs.put("password", "");
    configs.put("normalConfig", "value");

    String obfuscatedJson = Obfuscator.DEFAULT.toJsonString(configs);

    // Verify that null and empty values are handled properly
    Assert.assertTrue(obfuscatedJson.contains("\"authToken\":\"*****\""));
    Assert.assertTrue(obfuscatedJson.contains("\"password\":\"*****\""));
    Assert.assertTrue(obfuscatedJson.contains("\"normalConfig\":\"value\""));
  }
}
