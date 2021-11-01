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
package org.apache.pinot.core.query.utils.idset;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.longlong.LongIterator;


/**
 * The class {@code IdSets} contains the utility methods for the {@link IdSet}.
 */
@SuppressWarnings({"unchecked", "UnstableApiUsage"})
public class IdSets {
  private IdSets() {
  }

  // Use 8MB as the default threshold to convert to BloomFilter
  public static final int DEFAULT_SIZE_THRESHOLD_IN_BYTES = 8 * 1024 * 1024;
  // NOTE: With default expectedInsertions and fpp, the BloomFilter will be around 4.35MB.
  public static final int DEFAULT_EXPECTED_INSERTIONS = 5_000_000;
  public static final double DEFAULT_FPP = 0.03;

  /**
   * Creates an IdSet.
   *
   * @param dataType Data type of the ids
   * @return Created IdSet
   * */
  public static IdSet create(DataType dataType) {
    return create(dataType, DEFAULT_SIZE_THRESHOLD_IN_BYTES, DEFAULT_EXPECTED_INSERTIONS, DEFAULT_FPP);
  }

  /**
   * Creates an IdSet.
   *
   * @param dataType Data type of the ids
   * @param sizeThresholdInBytes Size threshold in bytes to convert to BloomFilterIdSet. Directly create
   *                             BloomFilterIdSet if it is smaller or equal to 0
   * @param expectedInsertions Number of expected insertions for the BloomFilter, must be positive
   * @param fpp Desired false positive probability for the BloomFilter, must be positive and less than 1.0
   * @return Created IdSet
   */
  public static IdSet create(DataType dataType, int sizeThresholdInBytes, int expectedInsertions, double fpp) {
    if (sizeThresholdInBytes <= 0) {
      return new BloomFilterIdSet(dataType, expectedInsertions, fpp);
    }
    switch (dataType) {
      case INT:
        return new RoaringBitmapIdSet();
      case LONG:
        return new Roaring64NavigableMapIdSet();
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BYTES:
      case BIGDECIMAL:
        return new BloomFilterIdSet(dataType, expectedInsertions, fpp);
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  /**
   * Returns an EmptyIdSet.
   */
  public static EmptyIdSet emptyIdSet() {
    return EmptyIdSet.INSTANCE;
  }

  /**
   * Merges 2 IdSets, converts to BloomFilterIdSet if the size exceeds the size threshold.
   *
   * @param idSet1 First IdSet to merge
   * @param idSet2 Second IdSet to merge
   * @return Merged IdSet
   */
  public static IdSet merge(IdSet idSet1, IdSet idSet2) {
    return merge(idSet1, idSet2, DEFAULT_SIZE_THRESHOLD_IN_BYTES, DEFAULT_EXPECTED_INSERTIONS, DEFAULT_FPP);
  }

  /**
   * Merges 2 IdSets, converts to BloomFilterIdSet if the size exceeds the size threshold.
   *
   * @param idSet1 First IdSet to merge
   * @param idSet2 Second IdSet to merge
   * @param sizeThresholdInBytes Size threshold in bytes to convert to BloomFilterIdSet. Directly create
   *                             BloomFilterIdSet if it is smaller or equal to 0
   * @param expectedInsertions Number of expected insertions for the BloomFilter, must be positive
   * @param fpp Desired false positive probability for the BloomFilter, must be positive and less than 1.0
   * @return Merged IdSet
   */
  public static IdSet merge(IdSet idSet1, IdSet idSet2, int sizeThresholdInBytes, int expectedInsertions, double fpp) {
    IdSet.Type idSet1Type = idSet1.getType();
    if (idSet1Type == IdSet.Type.EMPTY) {
      return idSet2;
    }
    IdSet.Type idSet2Type = idSet2.getType();
    if (idSet2Type == IdSet.Type.EMPTY) {
      return idSet1;
    }
    if (idSet1Type == idSet2Type) {
      switch (idSet1Type) {
        case ROARING_BITMAP:
          RoaringBitmapIdSet roaringBitmapIdSet = (RoaringBitmapIdSet) idSet1;
          roaringBitmapIdSet.getBitmap().or(((RoaringBitmapIdSet) idSet2).getBitmap());
          return convertToBloomFilterIdSetIfNeeded(roaringBitmapIdSet, sizeThresholdInBytes, expectedInsertions, fpp);
        case ROARING_64_NAVIGABLE_MAP:
          Roaring64NavigableMapIdSet roaring64NavigableMapIdSet = (Roaring64NavigableMapIdSet) idSet1;
          roaring64NavigableMapIdSet.getBitmap().or(((Roaring64NavigableMapIdSet) idSet2).getBitmap());
          return convertToBloomFilterIdSetIfNeeded(roaring64NavigableMapIdSet, sizeThresholdInBytes, expectedInsertions,
              fpp);
        case BLOOM_FILTER:
          BloomFilterIdSet bloomFilterIdSet = (BloomFilterIdSet) idSet1;
          bloomFilterIdSet.getBloomFilter().putAll(((BloomFilterIdSet) idSet2).getBloomFilter());
          return bloomFilterIdSet;
        default:
          throw new IllegalStateException();
      }
    }
    // One of the IdSet is BloomFilterIdSet, the other one is not
    BloomFilterIdSet bloomFilterIdSet;
    IdSet nonBloomFilterIdSet;
    IdSet.Type nonBloomFilterIdSetType;
    if (idSet1Type == IdSet.Type.BLOOM_FILTER) {
      bloomFilterIdSet = (BloomFilterIdSet) idSet1;
      nonBloomFilterIdSet = idSet2;
      nonBloomFilterIdSetType = idSet2Type;
    } else {
      assert idSet2Type == IdSet.Type.BLOOM_FILTER;
      bloomFilterIdSet = (BloomFilterIdSet) idSet2;
      nonBloomFilterIdSet = idSet1;
      nonBloomFilterIdSetType = idSet1Type;
    }
    if (nonBloomFilterIdSetType == IdSet.Type.ROARING_BITMAP) {
      PeekableIntIterator intIterator = ((RoaringBitmapIdSet) nonBloomFilterIdSet).getBitmap().getIntIterator();
      while (intIterator.hasNext()) {
        bloomFilterIdSet.add(intIterator.next());
      }
    } else {
      assert nonBloomFilterIdSetType == IdSet.Type.ROARING_64_NAVIGABLE_MAP;
      LongIterator longIterator = ((Roaring64NavigableMapIdSet) nonBloomFilterIdSet).getBitmap().getLongIterator();
      while (longIterator.hasNext()) {
        bloomFilterIdSet.add(longIterator.next());
      }
    }
    return bloomFilterIdSet;
  }

  /**
   * Helper method to convert the RoaringBitmapIdSet to BloomFilterIdSet if it exceeds the size threshold.
   */
  private static IdSet convertToBloomFilterIdSetIfNeeded(RoaringBitmapIdSet roaringBitmapIdSet, int sizeThreshold,
      int expectedInsertions, double fpp) {
    if (roaringBitmapIdSet.getSerializedSizeInBytes() <= sizeThreshold) {
      return roaringBitmapIdSet;
    }
    BloomFilterIdSet bloomFilterIdSet = new BloomFilterIdSet(DataType.INT, expectedInsertions, fpp);
    PeekableIntIterator intIterator = roaringBitmapIdSet.getBitmap().getIntIterator();
    while (intIterator.hasNext()) {
      bloomFilterIdSet.add(intIterator.next());
    }
    return bloomFilterIdSet;
  }

  /**
   * Helper method to convert the Roaring64NavigableMapIdSet to BloomFilterIdSet if it exceeds the size threshold.
   */
  private static IdSet convertToBloomFilterIdSetIfNeeded(Roaring64NavigableMapIdSet roaring64NavigableMapIdSet,
      int sizeThreshold, int expectedInsertions, double fpp) {
    if (roaring64NavigableMapIdSet.getSerializedSizeInBytes() <= sizeThreshold) {
      return roaring64NavigableMapIdSet;
    }
    BloomFilterIdSet bloomFilterIdSet = new BloomFilterIdSet(DataType.LONG, expectedInsertions, fpp);
    LongIterator longIterator = roaring64NavigableMapIdSet.getBitmap().getLongIterator();
    while (longIterator.hasNext()) {
      bloomFilterIdSet.add(longIterator.next());
    }
    return bloomFilterIdSet;
  }

  /**
   * Deserializes the IdSet from a byte array.
   */
  public static IdSet fromBytes(byte[] bytes)
      throws IOException {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  /**
   * Deserializes the IdSet from a ByteBuffer.
   */
  public static IdSet fromByteBuffer(ByteBuffer byteBuffer)
      throws IOException {
    byte typeId = byteBuffer.get();
    switch (typeId) {
      case 0:
        return EmptyIdSet.INSTANCE;
      case 1:
        return RoaringBitmapIdSet.fromByteBuffer(byteBuffer);
      case 2:
        return Roaring64NavigableMapIdSet.fromByteBuffer(byteBuffer);
      case 3:
        return BloomFilterIdSet.fromByteBuffer(byteBuffer);
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Deserializes the IdSet from a Base64 string.
   * <p>Use Base64 instead of Hex encoding for better compression.
   */
  public static IdSet fromBase64String(String base64String)
      throws IOException {
    return fromBytes(Base64.getDecoder().decode(base64String));
  }
}
