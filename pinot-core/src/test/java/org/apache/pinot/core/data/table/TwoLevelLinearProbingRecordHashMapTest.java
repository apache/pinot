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
package org.apache.pinot.core.data.table;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class TwoLevelLinearProbingRecordHashMapTest {

  @Test
  public void testBasicPutAndGet() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    Key key1 = new Key(new Object[]{1});
    Record val1 = new Record(new Object[]{"one"});
    map.put(key1, val1);

    assertEquals(1, map.size());
    assertEquals(val1, map.get(key1));
  }

  @Test
  public void testOverwriteValue() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    Key key = new Key(new Object[]{42});
    map.put(key, new Record(new Object[]{"a"}));
    map.put(key, new Record(new Object[]{"b"}));

    assertEquals(1, map.size());
    assertEquals(new Record(new Object[]{"b"}), map.get(key));
  }

  @Test
  public void testGetNonExistentKey() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    assertNull(map.get(new Key(new Object[]{999})));
  }

  @Test
  public void testResizeWithMultipleKeys() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    int count = 100;
    for (int i = 0; i < count; i++) {
      map.put(new Key(new Object[]{i}), new Record(new Object[]{"v" + i}));
    }
    assertEquals(count, map.size());

    for (int i = 0; i < count; i++) {
      assertEquals(new Record(new Object[]{"v" + i}), map.get(new Key(new Object[]{i})));
    }
  }

  @Test
  public void testHashCollisionHandling() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();

    Key key1 = new Key(new Object[]{1}) {
      @Override
      public int hashCode() {
        return 42;
      }

      @Override
      public boolean equals(Object o) {
        return super.equals(o);
      }
    };
    Key key2 = new Key(new Object[]{2}) {
      @Override
      public int hashCode() {
        return 42;
      }

      @Override
      public boolean equals(Object o) {
        return super.equals(o);
      }
    };

    Record r1 = new Record(new Object[]{"r1"});
    Record r2 = new Record(new Object[]{"r2"});

    map.put(key1, r1);
    map.put(key2, r2);

    assertEquals(r1, map.get(key1));
    assertEquals(r2, map.get(key2));
  }

  @Test
  public void testPutAfterResize() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    for (int i = 0; i < 2000; i++) {
      map.put(new Key(new Object[]{i}), new Record(new Object[]{"v" + i}));
    }
    Key newKey = new Key(new Object[]{1000});
    Record newVal = new Record(new Object[]{"after_resize"});

    map.put(newKey, newVal);
    assertEquals(newVal, map.get(newKey));
  }

  @Test
  public void testPutLargeSize() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    for (int i = 0; i < 100000; i++) {
      map.put(new Key(new Object[]{i}), new Record(new Object[]{"v" + i}));
    }
    assertEquals(map.size(), 100000);
    Key newKey = new Key(new Object[]{1000});
    Record newVal = new Record(new Object[]{"after_resize"});

    map.put(newKey, newVal);
    assertEquals(newVal, map.get(newKey));
  }

  @Test
  public void testEmptyMap() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    assertEquals(0, map.size());
    assertNull(map.get(new Key(new Object[]{0})));
  }

  @Test
  public void testSingleInsertAndRetrieve() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    Key key = new Key(new Object[]{1});
    Record value = new Record(new Object[]{"value"});
    map.put(key, value);
    assertEquals(1, map.size());
    assertEquals(value, map.get(key));
  }

  @Test
  public void testMultipleInsertsSameBucket() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    for (int i = 0; i < 100; i++) {
      map.put(new Key(new Object[]{i}), new Record(new Object[]{"val" + i}));
    }
    assertEquals(100, map.size());
    for (int i = 0; i < 100; i++) {
      assertEquals(new Record(new Object[]{"val" + i}), map.get(new Key(new Object[]{i})));
    }
  }

  @Test
  public void testOverwritingExistingKey() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    Key key = new Key(new Object[]{42});
    map.put(key, new Record(new Object[]{"first"}));
    map.put(key, new Record(new Object[]{"second"}));
    assertEquals(1, map.size());
    assertEquals(new Record(new Object[]{"second"}), map.get(key));
  }

  @Test
  public void testHashCollisionDifferentKeys() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    Key key1 = new Key(new Object[]{"a"}) {
      @Override
      public int hashCode() {
        return 123;
      }

      @Override
      public boolean equals(Object o) {
        return super.equals(o);
      }
    };
    Key key2 = new Key(new Object[]{"b"}) {
      @Override
      public int hashCode() {
        return 123;
      }

      @Override
      public boolean equals(Object o) {
        return super.equals(o);
      }
    };
    Record val1 = new Record(new Object[]{"valA"});
    Record val2 = new Record(new Object[]{"valB"});

    map.put(key1, val1);
    map.put(key2, val2);

    assertEquals(val1, map.get(key1));
    assertEquals(val2, map.get(key2));
    assertEquals(2, map.size());
  }

  @Test
  public void testRetrieveAfterResize() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    int count = 500; // Force multiple resizes
    for (int i = 0; i < count; i++) {
      map.put(new Key(new Object[]{i}), new Record(new Object[]{"v" + i}));
    }
    assertEquals(count, map.size());

    for (int i = 0; i < count; i++) {
      assertEquals(new Record(new Object[]{"v" + i}), map.get(new Key(new Object[]{i})));
    }
  }

  @Test
  public void testKeyObjectArrayEquality() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    Key k1 = new Key(new Object[]{"a", 1});
    Key k2 = new Key(new Object[]{"a", 1});
    Record r = new Record(new Object[]{"test"});
    map.put(k1, r);
    assertEquals(r, map.get(k2));
  }

  @Test
  public void testRecordObjectArrayEquality() {
    Record r1 = new Record(new Object[]{"a", 100});
    Record r2 = new Record(new Object[]{"a", 100});
    assertEquals(r1, r2);
  }

  @Test
  public void testNullRecordValue() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    Key key = new Key(new Object[]{999});
    map.put(key, null);  // Intentionally put null
    assertNull(map.get(key));
    assertEquals(1, map.size());
  }

  @Test
  public void testDuplicateInsertMany() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    Key key = new Key(new Object[]{123});
    for (int i = 0; i < 100; i++) {
      map.put(key, new Record(new Object[]{"v" + i}));
    }
    assertEquals(1, map.size());
    assertEquals(new Record(new Object[]{"v99"}), map.get(key));
  }

  @Test
  public void testStressWithSimilarKeys() {
    TwoLevelLinearProbingRecordHashMap map = new TwoLevelLinearProbingRecordHashMap();
    for (int i = 0; i < 256; i++) {
      map.put(new Key(new Object[]{"group", i}), new Record(new Object[]{"r" + i}));
    }
    for (int i = 0; i < 256; i++) {
      assertEquals(new Record(new Object[]{"r" + i}), map.get(new Key(new Object[]{"group", i})));
    }
    assertEquals(256, map.size());
  }
}
