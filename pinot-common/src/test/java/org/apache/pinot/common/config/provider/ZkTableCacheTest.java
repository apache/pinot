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
package org.apache.pinot.common.config.provider;

import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Exercises hybrid-table removal through registered ZK callbacks; each test owns its cache and mock store.
public class ZkTableCacheTest {
  @Test
  @SuppressWarnings("unchecked") // Mockito's class token cannot retain the property store's ZNRecord type parameter.
  public void testCaseSensitiveHybridNameSurvivesUntilLastTableIsRemoved()
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> store = mock(ZkHelixPropertyStore.class);
    ZkTableCache cache = new ZkTableCache(store, false);
    ArgumentCaptor<IZkChildListener> childListener = ArgumentCaptor.forClass(IZkChildListener.class);
    verify(store).subscribeChildChanges(eq("/CONFIGS/TABLE"), childListener.capture());
    IZkDataListener listener = (IZkDataListener) childListener.getValue();
    TableConfig offline = new TableConfigBuilder(TableType.OFFLINE).setTableName("MixedCaseTable").build();
    TableConfig realtime = new TableConfigBuilder(TableType.REALTIME).setTableName("MixedCaseTable").build();
    String offlinePath = "/CONFIGS/TABLE/" + offline.getTableName();
    String realtimePath = "/CONFIGS/TABLE/" + realtime.getTableName();
    listener.handleDataChange(offlinePath, TableConfigSerDeUtils.toZNRecord(offline));
    listener.handleDataChange(realtimePath, TableConfigSerDeUtils.toZNRecord(realtime));
    assertEquals(cache.getActualTableName("MixedCaseTable"), "MixedCaseTable");
    assertNull(cache.getActualTableName("mixedcasetable"));

    listener.handleDataDeleted("/cluster/PROPERTYSTORE" + realtimePath);
    assertNull(cache.getTableConfig(realtime.getTableName()));
    assertNull(cache.getActualTableName(realtime.getTableName()));
    assertEquals(cache.getTableConfig(offline.getTableName()), offline);
    assertEquals(cache.getActualTableName("MixedCaseTable"), "MixedCaseTable");

    listener.handleDataChange(realtimePath, TableConfigSerDeUtils.toZNRecord(realtime));
    listener.handleDataDeleted("/cluster/PROPERTYSTORE" + offlinePath);
    assertNull(cache.getTableConfig(offline.getTableName()));
    assertEquals(cache.getTableConfig(realtime.getTableName()), realtime);
    assertEquals(cache.getActualTableName("MixedCaseTable"), "MixedCaseTable");

    listener.handleDataDeleted("/cluster/PROPERTYSTORE" + realtimePath);
    assertNull(cache.getTableConfig(realtime.getTableName()));
    assertNull(cache.getActualTableName("MixedCaseTable"));
    assertTrue(cache.getTableNameMap().isEmpty());
    verify(store).unsubscribeDataChanges(offlinePath, listener);
    verify(store, times(2)).unsubscribeDataChanges(realtimePath, listener);
  }
}
