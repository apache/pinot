/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.indexsegment.utils;

import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.OffHeapStringStore;


public class OffHeapStringStoreTest {
  private static Random RANDOM = new Random();
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(OffHeapStringStoreTest.class.getName());
  }

  @AfterClass
  public void tearDown() throws Exception {
    _memoryManager.close();
  }

  @Test
  public void maxValueTest() throws Exception {
    OffHeapStringStore store = new OffHeapStringStore(_memoryManager, "stringColumn");
    final int arrSize = OffHeapStringStore.getStartSize() - 4;
    String dataIn = generateRandomString(arrSize);
    int index = store.add(dataIn);
    String dataOut = store.get(index);
    Assert.assertEquals(dataIn, dataOut);
    store.close();
  }

  @Test
  public void overflowTest() throws Exception {
    OffHeapStringStore store = new OffHeapStringStore(_memoryManager, "stringColumn");
    final int maxSize = OffHeapStringStore.getStartSize() - 4;

    String b1 = generateRandomString(3);
    String b2 = generateRandomString(maxSize);

    // Add small array
    final int i1 = store.add(b1);
    Assert.assertEquals(store.get(i1), b1);

    // And now the larger one, should result in a new buffer
    final int i2 = store.add(b2);
    Assert.assertEquals(store.get(i2), b2);

    // And now one more, should result in a new buffer but exact fit.
    final int i3 = store.add(b2);
    Assert.assertEquals(store.get(i3), b2);

    // One more buffer when we add the small one again.
    final int i4 = store.add(b1);
    Assert.assertEquals(store.get(i4), b1);

    // Test with one more 'get' to ensure that things have not changed.
    Assert.assertEquals(store.get(i1), b1);
    Assert.assertEquals(store.get(i2), b2);
    Assert.assertEquals(store.get(i3), b2);
    Assert.assertEquals(store.get(i4), b1);

    String b3 = generateRandomString(5);

    int ix = -1;
    final int iters = 1_000_000;

    // Now add the small one multiple times causing many additions.
    for (int i = 0; i < iters; i++) {
      if (ix == -1) {
        ix = store.add(b3);
      }
      store.add(b3);
    }
    for (int i = 0; i < iters; i++) {
      Assert.assertEquals(store.get(ix++), b3);
    }

    // Original values should still be good.
    Assert.assertEquals(store.get(i1), b1);
    Assert.assertEquals(store.get(i2), b2);
    Assert.assertEquals(store.get(i3), b2);
    Assert.assertEquals(store.get(i4), b1);
    store.close();
  }

  // Generates a ascii displayable string of given length
  private String generateRandomString(final int len) {
    byte[] bytes = new byte[len];
    for (int i = 0; i < len; i++) {
      bytes[i] = (byte)(RANDOM.nextInt(92) + 32);
    }
    return new String(bytes);
  }
}
