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
package org.apache.pinot.plugin.system;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.system.SystemMetadata;
import org.apache.pinot.spi.system.SystemMetadataRegistry;
import org.apache.pinot.spi.system.SystemMetadataStore;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SystemMetadataRegistryTest {
  private static final Map<String, Object> SYSTEM_METADATA_TEST_CONFIG = new HashMap<>();
  static {
    SYSTEM_METADATA_TEST_CONFIG.put("system.metadata.factory.class",
        "org.apache.pinot.plugin.system.sqlite.SqliteMetadataStoreFactory");
    SYSTEM_METADATA_TEST_CONFIG.put("system.metadata.sqlite.conn",
        "jdbc:sqlite::memory:");
  }

  @Test
  public void testSystemMetadataRegistry()
      throws Exception {
    PinotConfiguration pinotConfiguration = new PinotConfiguration(
        SYSTEM_METADATA_TEST_CONFIG
    );
    // 1. initialize system metadata registry.
    SystemMetadataRegistry systemMetadataRegistry = ComponentSystemMetadataRegistry.getInstance();
    systemMetadataRegistry.init(pinotConfiguration);

    // 2. register system metadata
    TestSystemMetadata systemMetadata = new TestSystemMetadata();
    systemMetadataRegistry.registerSystemMetadata(systemMetadata);

    // 3. get the system metadata store for posting data.
    SystemMetadataStore systemMetadataStore = systemMetadataRegistry.getSystemMetadataStore(
        systemMetadata.getSystemMetadataName());

    // 4. post data
    Object[] data = new Object[]{1L, "2S", 3L};
    systemMetadataStore.collect(systemMetadata, System.currentTimeMillis(), data);
    List<Object[]> rows = systemMetadataStore.inspect(systemMetadata);
    Assert.assertEquals(rows.size(), 1);
    Assert.assertEquals(rows.get(0), data);
  }

  private static class TestSystemMetadata implements SystemMetadata {
    private static final FieldSpec[] FIELD_SPECS = new FieldSpec[]{
        new DimensionFieldSpec("d", FieldSpec.DataType.LONG, true),
        new DimensionFieldSpec("s", FieldSpec.DataType.STRING, true),
        new MetricFieldSpec("m", FieldSpec.DataType.LONG)};
    private static final FieldSpec.DataType[] FIELD_TYPES = Arrays.stream(FIELD_SPECS)
        .map(FieldSpec::getDataType).collect(Collectors.toList()).toArray(new FieldSpec.DataType[0]);
    private static final String[] FIELD_NAMES = Arrays.stream(FIELD_SPECS)
        .map(FieldSpec::getName).collect(Collectors.toList()).toArray(new String[0]);

    @Override
    public String[] getColumnName() {
      return FIELD_NAMES;
    }

    @Override
    public FieldSpec.DataType[] getColumnDataType() {
      return FIELD_TYPES;
    }

    @Override
    public String getSystemMetadataName() {
      return "TestSystemMetadata";
    }
  }
}
