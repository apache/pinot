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
package org.apache.pinot.common.metadata;

import java.io.IOException;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Verifies strict raw table-config reads using real metadata serialization and isolated mocked stores.
/// Tests keep mutable state local to each invocation and can run concurrently.
public class ZKMetadataProviderTableConfigTest {
  private static final String TABLE_NAME = "source_OFFLINE";
  private static final String CONFIG_PATH = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(TABLE_NAME);

  @Test
  public void missingRecordReturnsNull()
      throws IOException {
    ZkHelixPropertyStore<ZNRecord> store = newStore();
    assertNull(ZKMetadataProvider.getRawTableConfig(store, TABLE_NAME));
    verify(store).get(CONFIG_PATH, null, AccessOption.PERSISTENT);
  }

  @DataProvider(name = "tableTypes")
  public Object[][] tableTypes() {
    return new Object[][]{{TableType.OFFLINE}, {TableType.REALTIME}};
  }

  @Test(dataProvider = "tableTypes")
  public void rawReadPreservesPersistedDefinitionAndPlaceholders(TableType type)
      throws IOException {
    TableConfig original = new TableConfigBuilder(type).setTableName("source")
        .setCustomConfig(new TableCustomConfig(Map.of("token", "${PINOT_STRICT_RAW_TOKEN:resolved}"))).build();
    String typedName = original.getTableName();
    String path = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(typedName);
    ZkHelixPropertyStore<ZNRecord> store = newStore();
    when(store.get(path, null, AccessOption.PERSISTENT)).thenReturn(TableConfigSerDeUtils.toZNRecord(original));

    TableConfig actual = ZKMetadataProvider.getRawTableConfig(store, typedName);

    assertEquals(actual, original);
    assertEquals(actual.getCustomConfig().getCustomConfigs().get("token"), "${PINOT_STRICT_RAW_TOKEN:resolved}");
    verify(store).get(path, null, AccessOption.PERSISTENT);
  }

  @DataProvider(name = "unreadableRecords")
  public Object[][] unreadableRecords() {
    return new Object[][]{{true, IOException.class}, {false, IllegalStateException.class}};
  }

  @Test(dataProvider = "unreadableRecords")
  public void unreadableRecordThrowsWhileLegacyAccessorRemainsTolerant(boolean invalidJson,
      Class<? extends Exception> expectedException)
      throws IOException {
    TableConfig original = new TableConfigBuilder(TableType.OFFLINE).setTableName("source").build();
    ZNRecord record = TableConfigSerDeUtils.toZNRecord(original);
    if (invalidJson) {
      record.setSimpleField(TableConfig.TENANT_CONFIG_KEY, "{invalid-json");
    } else {
      record.getSimpleFields().remove(TableConfig.TENANT_CONFIG_KEY);
    }
    ZkHelixPropertyStore<ZNRecord> store = newStore();
    when(store.get(CONFIG_PATH, null, AccessOption.PERSISTENT)).thenReturn(record);

    expectThrows(expectedException, () -> ZKMetadataProvider.getRawTableConfig(store, TABLE_NAME));
    assertNull(ZKMetadataProvider.getTableConfig(store, TABLE_NAME, false, false));
    verify(store, times(2)).get(CONFIG_PATH, null, AccessOption.PERSISTENT);
  }

  @Test
  public void storageFailureIsPropagated() {
    ZkHelixPropertyStore<ZNRecord> store = newStore();
    IllegalStateException failure = new IllegalStateException("storage read failed");
    when(store.get(CONFIG_PATH, null, AccessOption.PERSISTENT)).thenThrow(failure);

    assertSame(expectThrows(IllegalStateException.class,
        () -> ZKMetadataProvider.getRawTableConfig(store, TABLE_NAME)), failure);
  }

  // A Class literal cannot retain the property's ZNRecord type parameter.
  @SuppressWarnings("unchecked")
  private static ZkHelixPropertyStore<ZNRecord> newStore() {
    return mock(ZkHelixPropertyStore.class);
  }
}
