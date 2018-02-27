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
package com.linkedin.pinot.core.common.docidsets;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BitmapDocIdSetTest {

  @Test
  public void testSimple() throws IOException {
    int numBitMaps = 5;
    final int numDocs = 1000;
    List<ImmutableRoaringBitmap> list = new ArrayList<>();
    Random r = new Random();
    TreeSet<Integer> originalSet = new TreeSet<>();

    for (int i = 0; i < numBitMaps; i++) {
      MutableRoaringBitmap mutableRoaringBitmap = new MutableRoaringBitmap();
      int length = r.nextInt(numDocs);
      for (int j = 0; j < length; j++) {
        int docId = r.nextInt(numDocs);
        originalSet.add(docId);
        mutableRoaringBitmap.add(docId);
      }
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(bos);
      // could call "rr1.runOptimize()" and "rr2.runOptimize" if there
      // there were runs to compress
      mutableRoaringBitmap.serialize(dos);
      dos.close();
      ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
      ImmutableRoaringBitmap immutableRoaringBitmap = new ImmutableRoaringBitmap(bb);
      list.add(immutableRoaringBitmap);
    }
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[list.size()];
    list.toArray(bitmaps);
    BitmapDocIdSet bitmapDocIdSet = new BitmapDocIdSet(bitmaps, 0, numDocs - 1, false);
    BlockDocIdIterator iterator = bitmapDocIdSet.iterator();
    int docId;
    TreeSet<Integer> result = new TreeSet<>();

    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    Assert.assertEquals(originalSet.size(), result.size());
    Assert.assertEquals(originalSet, result);
  }
}
