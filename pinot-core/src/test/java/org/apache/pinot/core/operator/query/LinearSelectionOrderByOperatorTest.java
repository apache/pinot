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
package org.apache.pinot.core.operator.query;

import java.util.Comparator;
import java.util.List;
import org.apache.pinot.core.operator.query.LinearSelectionOrderByOperator.PartiallySortedListBuilder;
import org.apache.pinot.core.operator.query.LinearSelectionOrderByOperator.TotallySortedListBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class LinearSelectionOrderByOperatorTest {

  @Test
  public void testTotallySortedListBuilder() {
    int maxNumRows = 10;

    // Enough rows collected
    TotallySortedListBuilder listBuilder = new TotallySortedListBuilder(maxNumRows);
    for (int i = 0; i < maxNumRows; i++) {
      Object[] row = new Object[]{i / 2};
      boolean enoughRowsCollected = listBuilder.add(row);
      assertEquals(enoughRowsCollected, i == maxNumRows - 1);
    }
    List<Object[]> rows = listBuilder.build();
    assertEquals(rows.size(), maxNumRows);
    for (int i = 0; i < maxNumRows; i++) {
      assertEquals(rows.get(i), new Object[]{i / 2});
    }

    // Not enough rows collected
    listBuilder = new TotallySortedListBuilder(maxNumRows);
    for (int i = 0; i < maxNumRows - 1; i++) {
      Object[] row = new Object[]{i / 2};
      assertFalse(listBuilder.add(row));
    }
    rows = listBuilder.build();
    assertEquals(rows.size(), maxNumRows - 1);
    for (int i = 0; i < maxNumRows - 1; i++) {
      assertEquals(rows.get(i), new Object[]{i / 2});
    }
  }

  @Test
  public void testPartiallySortedListBuilder() {
    int maxNumRows = 10;
    Comparator<Object[]> partitionComparator = Comparator.comparingInt(row -> (Integer) row[0]);
    Comparator<Object[]> unsortedComparator = Comparator.comparingInt(row -> (Integer) row[1]);

    // Enough rows collected without tie rows
    PartiallySortedListBuilder listBuilder =
        new PartiallySortedListBuilder(maxNumRows, partitionComparator, unsortedComparator);
    for (int i = 0; i < maxNumRows; i++) {
      Object[] row = new Object[]{i / 2, maxNumRows - i};
      assertFalse(listBuilder.add(row));
    }
    int lastPartitionValue = (maxNumRows - 1) / 2;
    assertTrue(listBuilder.add(new Object[]{lastPartitionValue + 1, 0}));
    List<Object[]> rows = listBuilder.build();
    assertEquals(rows.size(), maxNumRows);
    for (int i = 0; i < maxNumRows; i++) {
      assertEquals(rows.get(i), new Object[]{i / 2, i % 2 == 0 ? maxNumRows - i - 1 : maxNumRows - i + 1});
    }

    // Enough rows collected with tie rows
    listBuilder = new PartiallySortedListBuilder(maxNumRows, partitionComparator, unsortedComparator);
    for (int i = 0; i < maxNumRows; i++) {
      Object[] row = new Object[]{i / 2, maxNumRows - i};
      assertFalse(listBuilder.add(row));
    }
    assertFalse(listBuilder.add(new Object[]{lastPartitionValue, 0}));
    assertFalse(listBuilder.add(new Object[]{lastPartitionValue, 2}));
    assertFalse(listBuilder.add(new Object[]{lastPartitionValue, 4}));
    assertFalse(listBuilder.add(new Object[]{lastPartitionValue, 6}));
    assertTrue(listBuilder.add(new Object[]{lastPartitionValue + 1, 0}));
    rows = listBuilder.build();
    assertEquals(rows.size(), maxNumRows);
    // For the last partition, should contain unsorted value 0 and 1
    for (int i = 0; i < maxNumRows; i++) {
      if (i / 2 != lastPartitionValue) {
        assertEquals(rows.get(i), new Object[]{i / 2, i % 2 == 0 ? maxNumRows - i - 1 : maxNumRows - i + 1});
      } else {
        assertEquals(rows.get(i), new Object[]{lastPartitionValue, i % 2});
      }
    }

    // Not enough rows collected with tie rows
    listBuilder = new PartiallySortedListBuilder(maxNumRows, partitionComparator, unsortedComparator);
    for (int i = 0; i < maxNumRows; i++) {
      Object[] row = new Object[]{i / 2, maxNumRows - i};
      assertFalse(listBuilder.add(row));
    }
    assertFalse(listBuilder.add(new Object[]{lastPartitionValue, 0}));
    assertFalse(listBuilder.add(new Object[]{lastPartitionValue, 2}));
    assertFalse(listBuilder.add(new Object[]{lastPartitionValue, 4}));
    assertFalse(listBuilder.add(new Object[]{lastPartitionValue, 6}));
    rows = listBuilder.build();
    assertEquals(rows.size(), maxNumRows);
    // For the last partition, should contain unsorted value 0 and 1
    for (int i = 0; i < maxNumRows; i++) {
      if (i / 2 != lastPartitionValue) {
        assertEquals(rows.get(i), new Object[]{i / 2, i % 2 == 0 ? maxNumRows - i - 1 : maxNumRows - i + 1});
      } else {
        assertEquals(rows.get(i), new Object[]{lastPartitionValue, i % 2});
      }
    }
  }
}
