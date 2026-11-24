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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests schema child and data notifications in the ZooKeeper-backed table cache.
public class ZkTableCacheTest {
  @Test
  @SuppressWarnings("deprecation") // The cache currently calls the deprecated bulk-get overload.
  public void testEmptySchemaChildrenAndLastSchemaDeletion()
      throws Exception {
    String schemaName = "testSchema";
    String schemaPath = "/SCHEMAS/" + schemaName;
    Schema schema = new Schema.SchemaBuilder().setSchemaName(schemaName)
        .addSingleValueDimension("column", DataType.INT).build();

    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.getChildNames("/SCHEMAS", AccessOption.PERSISTENT)).thenReturn(List.of(schemaName));
    when(propertyStore.get(eq(List.of(schemaPath)), isNull(), eq(AccessOption.PERSISTENT)))
        .thenReturn(List.of(SchemaSerDeUtils.toZNRecord(schema)));

    ZkTableCache tableCache = new ZkTableCache(propertyStore, false);
    ArgumentCaptor<IZkChildListener> childListener = ArgumentCaptor.forClass(IZkChildListener.class);
    verify(propertyStore).subscribeChildChanges(eq("/SCHEMAS"), childListener.capture());
    IZkChildListener schemaChildListener = childListener.getValue();
    verify(propertyStore).subscribeDataChanges(eq(schemaPath), same((IZkDataListener) schemaChildListener));
    List<List<String>> notifications = new ArrayList<>();
    assertTrue(tableCache.registerSchemaChangeListener(schemas ->
        notifications.add(schemas.stream().map(Schema::getSchemaName).collect(Collectors.toList()))));
    assertNotNull(tableCache.getSchema(schemaName));
    assertEquals(notifications, List.of(List.of(schemaName)));

    // A child-list event may precede the data-deleted event. It must not remove the schema prematurely.
    schemaChildListener.handleChildChange("/SCHEMAS", List.of());
    schemaChildListener.handleChildChange("/SCHEMAS", null);
    assertNotNull(tableCache.getSchema(schemaName));
    assertEquals(notifications, List.of(List.of(schemaName)));
    verify(propertyStore, times(1)).get(eq(List.of(schemaPath)), isNull(), eq(AccessOption.PERSISTENT));

    ((IZkDataListener) schemaChildListener).handleDataDeleted("/cluster/PROPERTYSTORE" + schemaPath);
    assertNull(tableCache.getSchema(schemaName));
    assertEquals(notifications, List.of(List.of(schemaName), List.of()));

    // The child-list event can also arrive after deletion without sending a duplicate notification.
    schemaChildListener.handleChildChange("/SCHEMAS", List.of());
    assertEquals(notifications, List.of(List.of(schemaName), List.of()));
  }
}
