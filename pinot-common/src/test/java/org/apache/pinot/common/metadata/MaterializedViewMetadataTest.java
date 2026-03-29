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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.minion.MaterializedViewMetadata;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class MaterializedViewMetadataTest {

  @Test
  public void testZNRecordRoundTrip() {
    String mvTableName = "mv_daily_order_amount_OFFLINE";

    List<String> baseTables = Arrays.asList("orders_OFFLINE", "products_OFFLINE");
    String timeRangeRefTable = "orders_OFFLINE";

    Map<Integer, Set<Integer>> baseToMvPartitionMap = new HashMap<>();
    baseToMvPartitionMap.put(0, new HashSet<>(Arrays.asList(0, 1)));
    baseToMvPartitionMap.put(1, new HashSet<>(Collections.singletonList(2)));

    Map<Integer, Set<Integer>> mvToBasePartitionMap = new HashMap<>();
    mvToBasePartitionMap.put(0, new HashSet<>(Collections.singletonList(0)));
    mvToBasePartitionMap.put(1, new HashSet<>(Collections.singletonList(0)));
    mvToBasePartitionMap.put(2, new HashSet<>(Collections.singletonList(1)));

    String definedSql = "SELECT city, count(*) as cnt FROM orders GROUP BY city";

    MaterializedViewMetadata original = new MaterializedViewMetadata(
        mvTableName, baseTables, timeRangeRefTable, definedSql, baseToMvPartitionMap, mvToBasePartitionMap);

    ZNRecord znRecord = original.toZNRecord();
    assertEquals(znRecord.getId(), mvTableName);

    MaterializedViewMetadata restored = MaterializedViewMetadata.fromZNRecord(znRecord);
    assertEquals(restored.getMvTableNameWithType(), mvTableName);
    assertEquals(restored.getBaseTables(), Arrays.asList("orders_OFFLINE", "products_OFFLINE"));
    assertEquals(restored.getTimeRangeRefTable(), "orders_OFFLINE");
    assertEquals(restored.getDefinedSql(), definedSql);

    // Verify partition maps
    assertEquals(restored.getBaseToMvPartitionMap().get(0), new HashSet<>(Arrays.asList(0, 1)));
    assertEquals(restored.getBaseToMvPartitionMap().get(1), new HashSet<>(Collections.singletonList(2)));

    assertEquals(restored.getMvToBasePartitionMap().get(0), new HashSet<>(Collections.singletonList(0)));
    assertEquals(restored.getMvToBasePartitionMap().get(2), new HashSet<>(Collections.singletonList(1)));
  }

  @Test
  public void testEmptyPartitionMaps() {
    MaterializedViewMetadata metadata = new MaterializedViewMetadata(
        "mv_OFFLINE",
        Collections.singletonList("src_OFFLINE"),
        "src_OFFLINE",
        null,
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    ZNRecord znRecord = metadata.toZNRecord();
    MaterializedViewMetadata restored = MaterializedViewMetadata.fromZNRecord(znRecord);

    assertEquals(restored.getBaseTables(), Collections.singletonList("src_OFFLINE"));
    assertEquals(restored.getTimeRangeRefTable(), "src_OFFLINE");
    assertEquals(restored.getDefinedSql(), null);
    assertTrue(restored.getBaseToMvPartitionMap().isEmpty());
    assertTrue(restored.getMvToBasePartitionMap().isEmpty());
  }
}
