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
import java.util.Arrays;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.MutableOffHeapByteArrayStore;
import junit.framework.Assert;


public class MutableOffHeapByteArrayStoreTest {

  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(MutableOffHeapByteArrayStoreTest.class.getName());
  }

  @AfterClass
  public void tearDown() throws Exception {
    _memoryManager.close();
  }

  @Test
  public void maxValueTest() throws Exception {
    MutableOffHeapByteArrayStore store = new MutableOffHeapByteArrayStore(_memoryManager, "stringColumn", 1024, 32);
    final int arrSize = store.getStartSize();
    byte[] dataIn = new byte[arrSize-4];
    for (int i = 0; i < dataIn.length; i++) {
      dataIn[i] = (byte)(i % Byte.MAX_VALUE);
    }
    int index = store.add(dataIn);
    byte[] dataOut = store.get(index);
    Assert.assertTrue(Arrays.equals(dataIn, dataOut));
    store.close();
  }

  @Test
  public void overflowTest() throws Exception {
    MutableOffHeapByteArrayStore store = new MutableOffHeapByteArrayStore(_memoryManager, "stringColumn", 1024, 32);
    final int maxSize = store.getStartSize() - 4;

    byte[] b1 = new byte[3];
    for (int i = 0; i < b1.length; i++) {
      b1[i] = (byte)i;
    }

    byte[] b2 = new byte[maxSize];
    for (int i = 0; i < b2.length; i++) {
      b2[i] = (byte)(i % Byte.MAX_VALUE);
    }

    // Add small array
    final int i1 = store.add(b1);
    Assert.assertTrue(Arrays.equals(store.get(i1), b1));

    // And now the larger one, should result in a new buffer
    final int i2 = store.add(b2);
    Assert.assertTrue(Arrays.equals(store.get(i2), b2));

    // And now one more, should result in a new buffer but exact fit.
    final int i3 = store.add(b2);
    Assert.assertTrue(Arrays.equals(store.get(i3), b2));

    // One more buffer when we add the small one again.
    final int i4 = store.add(b1);
    Assert.assertTrue(Arrays.equals(store.get(i4), b1));

    // Test with one more 'get' to ensure that things have not changed.
    Assert.assertTrue(Arrays.equals(store.get(i1), b1));
    Assert.assertTrue(Arrays.equals(store.get(i2), b2));
    Assert.assertTrue(Arrays.equals(store.get(i3), b2));
    Assert.assertTrue(Arrays.equals(store.get(i4), b1));

    byte[] b3 = new byte[5];
    for (int i = 0; i < b3.length; i++) {
      b3[i] = (byte)(i+1);
    }

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
      Assert.assertTrue(Arrays.equals(store.get(ix++), b3));
    }

    // Original values should still be good.
    Assert.assertTrue(Arrays.equals(store.get(i1), b1));
    Assert.assertTrue(Arrays.equals(store.get(i2), b2));
    Assert.assertTrue(Arrays.equals(store.get(i3), b2));
    Assert.assertTrue(Arrays.equals(store.get(i4), b1));
    store.close();
  }
}
