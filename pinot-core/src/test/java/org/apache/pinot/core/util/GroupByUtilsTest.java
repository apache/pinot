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
package org.apache.pinot.core.util;

import org.apache.pinot.common.utils.HashUtil;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class GroupByUtilsTest {

  @Test
  public void testGetTableCapacity() {
    assertEquals(GroupByUtils.getTableCapacity(0), 5000);
    assertEquals(GroupByUtils.getTableCapacity(1), 5000);
    assertEquals(GroupByUtils.getTableCapacity(1000), 5000);
    assertEquals(GroupByUtils.getTableCapacity(10000), 50000);
    assertEquals(GroupByUtils.getTableCapacity(100000), 500000);
    assertEquals(GroupByUtils.getTableCapacity(1000000), 5000000);
    assertEquals(GroupByUtils.getTableCapacity(10000000), 50000000);
    assertEquals(GroupByUtils.getTableCapacity(100000000), 500000000);
    assertEquals(GroupByUtils.getTableCapacity(1000000000), Integer.MAX_VALUE);
  }

  @Test
  public void getIndexedTableTrimThreshold() {
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, -1), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 0), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 10), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 100), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 10000), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 100000), 100000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000000), 1000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 10000000), 10000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 100000000), 100000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000000000), 1000000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000000001), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(Integer.MAX_VALUE, 10), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(500000000, 10), 1000000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(500000001, 10), Integer.MAX_VALUE);
  }

  @Test
  public void testGetIndexedTableInitialCapacity() {
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 10, 128), 128);
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 100, 128),
        HashUtil.getHashMapCapacity(100));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 100, 256), 256);
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 1000, 256),
        HashUtil.getHashMapCapacity(1000));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 10, 128), 128);
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 10, 256), HashUtil.getHashMapCapacity(100));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 100, 256), HashUtil.getHashMapCapacity(100));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 1000, 256), HashUtil.getHashMapCapacity(100));
  }
}
