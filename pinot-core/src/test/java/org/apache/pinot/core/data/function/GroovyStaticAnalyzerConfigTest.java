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
package org.apache.pinot.core.data.function;

import java.util.Iterator;
import java.util.List;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.segment.local.function.GroovyStaticAnalyzerConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Test serialization and deserialization.
 */
public class GroovyStaticAnalyzerConfigTest {
  @Test
  public void testEmptyConfig() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false, null, null, null, null);
    String encodedConfig = JsonUtils.objectToString(config);
    GroovyStaticAnalyzerConfig decodedConfig =
        JsonUtils.stringToObject(encodedConfig, GroovyStaticAnalyzerConfig.class);

    Assert.assertFalse(decodedConfig.isEnabled());
    Assert.assertNull(decodedConfig.getAllowedReceivers());
    Assert.assertNull(decodedConfig.getAllowedImports());
    Assert.assertNull(decodedConfig.getAllowedStaticImports());
    Assert.assertNull(decodedConfig.getDisallowedMethodNames());
  }

  @Test
  public void testAllowedReceivers() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false,
        GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers(),
        null,
        null,
        null);
    String encodedConfig = JsonUtils.objectToString(config);
    GroovyStaticAnalyzerConfig decodedConfig =
        JsonUtils.stringToObject(encodedConfig, GroovyStaticAnalyzerConfig.class);

    Assert.assertFalse(decodedConfig.isEnabled());
    Assert.assertEquals(GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers(), decodedConfig.getAllowedReceivers());
    Assert.assertNull(decodedConfig.getAllowedImports());
    Assert.assertNull(decodedConfig.getAllowedStaticImports());
    Assert.assertNull(decodedConfig.getDisallowedMethodNames());
  }

  @Test
  public void testAllowedImports() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false,
        null,
        GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
        null,
        null);
    String encodedConfig = JsonUtils.objectToString(config);
    GroovyStaticAnalyzerConfig decodedConfig =
        JsonUtils.stringToObject(encodedConfig, GroovyStaticAnalyzerConfig.class);

    Assert.assertFalse(decodedConfig.isEnabled());
    Assert.assertNull(decodedConfig.getAllowedReceivers());
    Assert.assertEquals(GroovyStaticAnalyzerConfig.getDefaultAllowedImports(), decodedConfig.getAllowedImports());
    Assert.assertNull(decodedConfig.getAllowedStaticImports());
    Assert.assertNull(decodedConfig.getDisallowedMethodNames());
  }

  @Test
  public void testAllowedStaticImports() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false,
        null,
        null,
        GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
        null);
    String encodedConfig = JsonUtils.objectToString(config);
    GroovyStaticAnalyzerConfig decodedConfig =
        JsonUtils.stringToObject(encodedConfig, GroovyStaticAnalyzerConfig.class);

    Assert.assertFalse(decodedConfig.isEnabled());
    Assert.assertNull(decodedConfig.getAllowedReceivers());
    Assert.assertNull(decodedConfig.getAllowedImports());
    Assert.assertEquals(GroovyStaticAnalyzerConfig.getDefaultAllowedImports(), decodedConfig.getAllowedStaticImports());
    Assert.assertNull(decodedConfig.getDisallowedMethodNames());
  }

  @Test
  public void testDisallowedMethodNames() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false,
        null,
        null,
        null,
        List.of("method1", "method2"));
    String encodedConfig = JsonUtils.objectToString(config);
    GroovyStaticAnalyzerConfig decodedConfig =
        JsonUtils.stringToObject(encodedConfig, GroovyStaticAnalyzerConfig.class);

    Assert.assertFalse(decodedConfig.isEnabled());
    Assert.assertNull(decodedConfig.getAllowedReceivers());
    Assert.assertNull(decodedConfig.getAllowedImports());
    Assert.assertNull(decodedConfig.getAllowedStaticImports());
    Assert.assertEquals(List.of("method1", "method2"), decodedConfig.getDisallowedMethodNames());
  }

  @Test(dataProvider = "config_provider")
  public void testToZnRecord(GroovyStaticAnalyzerConfig config) throws Exception {
    ZNRecord zr = config.toZNRecord();
    GroovyStaticAnalyzerConfig znConfig = GroovyStaticAnalyzerConfig.fromZNRecord(zr);
    Assert.assertTrue(equals(znConfig, config));
  }

  @DataProvider(name = "config_provider")
  Iterator<GroovyStaticAnalyzerConfig> configProvider() {
    return List.of(
        new GroovyStaticAnalyzerConfig(false,
          null,
          null,
          null,
          List.of("method1", "method2")),
        new GroovyStaticAnalyzerConfig(false,
            GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers(),
            null,
            null,
            null),
        new GroovyStaticAnalyzerConfig(false,
            null,
            GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
            null,
            null),
        new GroovyStaticAnalyzerConfig(false,
            null,
            null,
            GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
            null),
        new GroovyStaticAnalyzerConfig(false,
            null,
            null,
            null,
            List.of("method1", "method2")),
        new GroovyStaticAnalyzerConfig(true,
            null,
            null,
            null,
            List.of("method1", "method2")),
        new GroovyStaticAnalyzerConfig(true,
            GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers(),
            null,
            null,
            null),
        new GroovyStaticAnalyzerConfig(true,
            null,
            GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
            null,
            null),
        new GroovyStaticAnalyzerConfig(true,
            null,
            null,
            GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
            null),
        new GroovyStaticAnalyzerConfig(true,
            null,
            null,
            null,
            List.of("method1", "method2"))
    ).iterator();
  }

  private boolean equals(GroovyStaticAnalyzerConfig a, GroovyStaticAnalyzerConfig b) {
    return a != null && b != null
        && a.isEnabled() == b.isEnabled()
        && (a.getAllowedStaticImports() == b.getAllowedStaticImports()
            || a.getAllowedStaticImports().equals(b.getAllowedStaticImports()))
        && (a.getAllowedImports() == null && b.getAllowedImports() == null
            || a.getAllowedImports().equals(b.getAllowedImports()))
        && (a.getAllowedReceivers() == null && b.getAllowedReceivers() == null
            || a.getAllowedReceivers().equals(b.getAllowedReceivers()))
        && (a.getDisallowedMethodNames() == null && b.getDisallowedMethodNames() == null
            || a.getDisallowedMethodNames().equals(b.getDisallowedMethodNames()));
  }
}
