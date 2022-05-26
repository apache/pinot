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
package org.apache.pinot.common.utils.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertTrue;


public class TableConfigTest {

  @DataProvider
  public Object[][] configs()
      throws IOException {
    try (Stream<Path> configs = Files.list(Paths.get("src/test/resources/testConfigs"))) {
      return configs.map(path -> {
            try {
              return Files.readAllBytes(path);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .map(config -> new Object[]{config})
          .toArray(Object[][]::new);
    }
  }

  @Test(dataProvider = "configs")
  public void testConfigNotRejected(byte[] config)
      throws IOException {
    TableConfig tableConfig = JsonUtils.DEFAULT_READER.forType(TableConfig.class).readValue(config);
    assertTrue(StringUtils.isNotBlank(tableConfig.getTableName()));
  }
}
