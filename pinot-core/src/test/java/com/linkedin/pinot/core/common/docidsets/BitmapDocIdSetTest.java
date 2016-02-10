/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class BitmapDocIdSetTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BitmapDocIdSetTest.class);

  @Test
  public void testSimple() throws IOException {
    int numBitMaps = 5;
    final int numDocs = 1000;
    List<ImmutableRoaringBitmap> list = new ArrayList<ImmutableRoaringBitmap>();
    Random r = new Random();
    TreeSet<Integer> originalSet = new TreeSet<Integer>();

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
    BlockMetadata blockMetadata = new BlockMetadata() {

      @Override
      public boolean isSparse() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean isSorted() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean isSingleValue() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean hasInvertedIndex() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean hasDictionary() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public int getStartDocId() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public int getSize() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public int getMaxNumberOfMultiValues() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public int getLength() {
        return numDocs;
      }

      @Override
      public int getEndDocId() {
        return numDocs - 1;
      }

      @Override
      public Dictionary getDictionary() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public DataType getDataType() {
        // TODO Auto-generated method stub
        return null;
      }
    };
    BitmapDocIdSet bitmapDocIdSet = new BitmapDocIdSet("testColumn", blockMetadata, 0, numDocs - 1, bitmaps);
    BlockDocIdIterator iterator = bitmapDocIdSet.iterator();
    int docId;
    TreeSet<Integer> result = new TreeSet<Integer>();

    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    Assert.assertEquals(originalSet.size(), result.size());
    Assert.assertEquals(originalSet, result);
  }
}
