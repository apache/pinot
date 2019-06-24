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
package org.apache.pinot.broker.requesthandler;

import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.QuotaConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableCustomConfig;
import org.apache.pinot.common.config.TagOverrideConfig;
import org.apache.pinot.common.utils.CommonConstants;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;


public class LazyTableConfigCacheTest {

  // Test is disabled as it relies on unreliable/sleep-based verification of async execution. Test is being included
  // regardless, so manual verifications can be done in the future if the class changes.
  @Test(enabled = false)
  public void testLazyLoading() throws Exception {

    String tableName = "tableName";
    ZkHelixPropertyStore<ZNRecord> mockStore = Mockito.mock(ZkHelixPropertyStore.class);
    TableConfig config = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(tableName)
        .setCustomConfig(new TableCustomConfig())
        .setQuotaConfig(new QuotaConfig()).setServerTenant("").setBrokerTenant("")
        .setTagOverrideConfig(new TagOverrideConfig()).build();
    Mockito.when(mockStore.get(anyString(), any(), anyInt())).thenReturn(config.toZNRecord());
    LazyTableConfigCache cache = new LazyTableConfigCache(mockStore);

    Assert.assertNull(cache.get(tableName));
    Thread.sleep(1000);
    Mockito.verify(mockStore, Mockito.atMost(1)).get(anyString(), any(), anyInt());
    // calling the cache a second time should find the value
    Assert.assertEquals(cache.get(tableName), config);
  }
}
