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
package org.apache.pinot.connector.flink.sink;

import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class PinotSinkAuthTest {
  // Dummy credentials: base64 of "user:pass"
  private static final String AUTH_TOKEN = "Basic dXNlcjpwYXNz";

  @Test
  public void testCreateAuthProviderWithToken() {
    TableConfig tableConfig = tableConfigWithBatchConfig(Map.of(BatchConfigProperties.AUTH_TOKEN, AUTH_TOKEN));

    AuthProvider authProvider = PinotSink.createAuthProvider(tableConfig);

    assertNotNull(authProvider);
    assertEquals(authProvider.getRequestHeaders().get("Authorization"), AUTH_TOKEN);
  }

  @Test
  public void testCreateAuthProviderWithoutIngestionConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();

    assertNull(PinotSink.createAuthProvider(tableConfig));
  }

  @Test
  public void testCreateAuthProviderWithoutBatchIngestionConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable")
        .setIngestionConfig(new IngestionConfig())
        .build();

    assertNull(PinotSink.createAuthProvider(tableConfig));
  }

  @Test
  public void testCreateAuthProviderWithoutToken() {
    TableConfig tableConfig = tableConfigWithBatchConfig(
        Map.of(BatchConfigProperties.PUSH_CONTROLLER_URI, "http://localhost:9000"));

    assertNull(PinotSink.createAuthProvider(tableConfig));
  }

  private static TableConfig tableConfigWithBatchConfig(Map<String, String> batchConfigMap) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(List.of(batchConfigMap), "APPEND", "DAILY"));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable")
        .setIngestionConfig(ingestionConfig)
        .build();
  }
}
