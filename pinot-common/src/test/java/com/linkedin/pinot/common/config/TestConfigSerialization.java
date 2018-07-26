/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.config;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.CombinedConfig;
import com.linkedin.pinot.common.config.CombinedConfigLoader;
import com.linkedin.pinot.common.config.Deserializer;
import com.linkedin.pinot.common.config.Serializer;
import com.linkedin.pinot.common.config.TableConfig;
import io.vavr.collection.Map;
import java.io.File;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for configuration serialization.
 */
public class TestConfigSerialization {
  @Test
  public void testConfigRoundtrip() throws Exception {
    // Load the config and check it
    CombinedConfig combinedConfig = CombinedConfigLoader
        .loadCombinedConfig(new File(getClass().getClassLoader().getResource("test-table-config.conf").getFile()));
    TableConfig config = combinedConfig.getOfflineTableConfig();
    validateLoadedConfig(config);

    // Shallow serialization (to a map of objects): serialize to a map of objects, deserialize and check config
    final Map<String, ?> serialize = Serializer.serialize(config);
    TableConfig newConfig = Deserializer.deserialize(TableConfig.class, serialize, "");

    Assert.assertEquals(config, newConfig);
    validateLoadedConfig(newConfig);

    // Serialization to a string (which could be written to a file): serialize to a string, deserialize and check config
    String configAsString = Serializer.serializeToString(config);
    TableConfig newConfigFromString = Deserializer.deserializeFromString(TableConfig.class, configAsString);

    if (!config.equals(newConfigFromString)) {
      System.out.println("Serialized config is = " + configAsString);
      System.out.println("Expected a config that contains: " + config);
    }

    Assert.assertEquals(config, newConfigFromString);
    validateLoadedConfig(newConfigFromString);
  }

  private void validateLoadedConfig(TableConfig config) {
    Assert.assertEquals(config.getTableName(), "mytable_OFFLINE");
    Assert.assertEquals(config.getQuotaConfig().getStorage(), "125 GiB");
    Assert.assertEquals(config.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(config.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    Assert.assertEquals(config.getTenantConfig().getBroker(), "foo");
    Assert.assertEquals(config.getTenantConfig().getServer(), "bar");
    Assert.assertEquals(config.getIndexingConfig().getSortedColumn(), Lists.newArrayList("foo"));
  }
}
