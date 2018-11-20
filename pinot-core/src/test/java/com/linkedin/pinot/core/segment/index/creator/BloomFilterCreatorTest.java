/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.creator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.google.common.hash.Funnels;

public class BloomFilterCreatorTest {

  @Test
  public void compareGauvaVsClearSpring() throws Exception {
    int numElementsArray[] = new int[] { 10000, 100000, 1000000 };
    for (int numElements : numElementsArray) {
      double maxFalsePosProbability = 0.1;
//      int optimalNumOfBits =com.google.common.hash.BloomFilter.optimalNumOfBits
      BloomFilter bloomFilter = new BloomFilter(numElements, maxFalsePosProbability);
      com.google.common.hash.BloomFilter<Integer> gauvaIntegerBloomFilter = com.google.common.hash.BloomFilter.create(Funnels.integerFunnel(), numElements,
          maxFalsePosProbability);
      com.google.common.hash.BloomFilter<String> gauvaStringBloomFilter = com.google.common.hash.BloomFilter
          .create(Funnels.stringFunnel(Charset.forName("UTF-8")), numElements, maxFalsePosProbability);
      System.out.println("BitSet size:" + bloomFilter.buckets());
      Random random = new Random();
      Set<Integer> set = new HashSet<>(); 
      for (int i = 0; i < 100000; i++) {
        int id = random.nextInt(500000000);
        set.add(id);
        bloomFilter.add(String.valueOf(id));
        gauvaIntegerBloomFilter.put(id);
        gauvaStringBloomFilter.put(String.valueOf(id));
      }
      byte[] buffer1 = BloomFilter.serialize(bloomFilter);
      ByteArrayOutputStream intBuffer = new ByteArrayOutputStream();
      gauvaIntegerBloomFilter.writeTo(intBuffer);
      ByteArrayOutputStream stringBuffer = new ByteArrayOutputStream();
      gauvaIntegerBloomFilter.writeTo(stringBuffer);
      //get the internal bitset
      System.out.println(Arrays.toString(BloomFilter.class.getFields()));
      Field field = BloomFilter.class.getDeclaredField("filter_");
      field.setAccessible(true);
      BitSet bitset = (BitSet) field.get(bloomFilter);
      MutableRoaringBitmap roaringBitmap = new MutableRoaringBitmap();
      for(long l:bitset.toLongArray()){
        roaringBitmap.add((int) l);
      }
      int roaringBitmapSize = roaringBitmap.serializedSizeInBytes();
      System.out.println("numBits:" + bitset.length());
      System.out.println("numBitsSet:" + bitset.cardinality());
      System.out.println("numElements:" + numElements);
      System.out.println("clear spring size:" + buffer1.length);
      System.out.println("Gauva int size:" + intBuffer.size());
      System.out.println("Gauva string size:" + stringBuffer.size());
      System.out.println("Roaring bitmap size:" + roaringBitmapSize);
      System.out.println("\n");
      BitSet bitSet;
    }

  }
}
