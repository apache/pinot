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

import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * The {@code BloomFilterIdSet} is an IdSet backed by the {@link BloomFilter}, and can be used to store all types of
 * ids.
 */
@SuppressWarnings({"rawtypes", "unchecked", "UnstableApiUsage"})
public class BloomFilterIdSet implements IdSet {

  private enum FunnelType {
    // DO NOT change the ids as the ser/de relies on them
    INT((byte) 0), LONG((byte) 1), STRING((byte) 2), BYTES((byte) 3);

    private final byte _id;

    FunnelType(byte id) {
      _id = id;
    }

    public byte getId() {
      return _id;
    }
  }

  private final FunnelType _funnelType;
  private final BloomFilter _bloomFilter;
  private final int _serializedSizeInBytes;

  BloomFilterIdSet(DataType dataType, int expectedInsertions, double fpp) {
    switch (dataType) {
      case INT:
      case FLOAT:
        _funnelType = FunnelType.INT;
        _bloomFilter = BloomFilter.create(Funnels.integerFunnel(), expectedInsertions, fpp);
        break;
      case LONG:
      case DOUBLE:
        _funnelType = FunnelType.LONG;
        _bloomFilter = BloomFilter.create(Funnels.longFunnel(), expectedInsertions, fpp);
        break;
      case STRING:
        _funnelType = FunnelType.STRING;
        _bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), expectedInsertions, fpp);
        break;
      case BYTES:
        _funnelType = FunnelType.BYTES;
        _bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), expectedInsertions, fpp);
        break;
      // TODO DDC big decimal stuff
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
    // NOTE: 1 byte for IdSet type, 1 byte for FunnelType, (2 bytes, 1 int, N longs) for BloomFilter.
    //       See BloomFilter.writeTo(OutputStream out) for details.
    long numBitsInBloomFilter = (long) (-expectedInsertions * Math.log(fpp) / (Math.log(2) * Math.log(2)));
    _serializedSizeInBytes =
        4 + Integer.BYTES + ((int) ((numBitsInBloomFilter + Long.SIZE - 1) / Long.SIZE) * Long.BYTES);
  }

  private BloomFilterIdSet(FunnelType funnelType, BloomFilter bloomFilter, int serializedSizeInBytes) {
    _funnelType = funnelType;
    _bloomFilter = bloomFilter;
    _serializedSizeInBytes = serializedSizeInBytes;
  }

  BloomFilter getBloomFilter() {
    return _bloomFilter;
  }

  @Override
  public Type getType() {
    return Type.BLOOM_FILTER;
  }

  @Override
  public void add(int id) {
    _bloomFilter.put(id);
  }

  @Override
  public void add(long id) {
    _bloomFilter.put(id);
  }

  @Override
  public void add(float id) {
    _bloomFilter.put(Float.floatToRawIntBits(id));
  }

  @Override
  public void add(double id) {
    _bloomFilter.put(Double.doubleToRawLongBits(id));
  }

  @Override
  public void add(String id) {
    _bloomFilter.put(id);
  }

  @Override
  public void add(byte[] id) {
    _bloomFilter.put(id);
  }

  @Override
  public void add(BigDecimal id) {
    _bloomFilter.put(id);
  }

  @Override
  public boolean contains(int id) {
    return _bloomFilter.mightContain(id);
  }

  @Override
  public boolean contains(long id) {
    return _bloomFilter.mightContain(id);
  }

  @Override
  public boolean contains(float id) {
    return _bloomFilter.mightContain(Float.floatToRawIntBits(id));
  }

  @Override
  public boolean contains(double id) {
    return _bloomFilter.mightContain(Double.doubleToRawLongBits(id));
  }

  @Override
  public boolean contains(String id) {
    return _bloomFilter.mightContain(id);
  }

  @Override
  public boolean contains(byte[] id) {
    return _bloomFilter.mightContain(id);
  }

  @Override
  public boolean contains(BigDecimal id) {
    return _bloomFilter.mightContain(id);
  }

  @Override
  public int getSerializedSizeInBytes() {
    return _serializedSizeInBytes;
  }

  @Override
  public byte[] toBytes()
      throws IOException {
    // NOTE: No need to close the stream.
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(_serializedSizeInBytes);
    byteArrayOutputStream.write(Type.BLOOM_FILTER.getId());
    byteArrayOutputStream.write(_funnelType.getId());
    _bloomFilter.writeTo(byteArrayOutputStream);
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Deserializes the BloomFilterIdSet from a ByteBuffer.
   * <p>NOTE: The ByteBuffer does not include the IdSet.Type byte.
   */
  static BloomFilterIdSet fromByteBuffer(ByteBuffer byteBuffer)
      throws IOException {
    Preconditions.checkArgument(byteBuffer.hasArray(),
        "Cannot deserialize BloomFilter from ByteBuffer not backed by an accessible byte array");
    // Count the IdSet.Type byte
    int serializedSizeInBytes = 1 + byteBuffer.remaining();
    byte funnelTypeId = byteBuffer.get();
    FunnelType funnelType;
    Funnel funnel;
    switch (funnelTypeId) {
      case 0:
        funnelType = FunnelType.INT;
        funnel = Funnels.integerFunnel();
        break;
      case 1:
        funnelType = FunnelType.LONG;
        funnel = Funnels.longFunnel();
        break;
      case 2:
        funnelType = FunnelType.STRING;
        funnel = Funnels.unencodedCharsFunnel();
        break;
      case 3:
        funnelType = FunnelType.BYTES;
        funnel = Funnels.byteArrayFunnel();
        break;
      default:
        throw new IllegalStateException();
    }
    // NOTE: No need to close the stream.
    BloomFilter bloomFilter = BloomFilter.readFrom(
        new ByteArrayInputStream(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(),
            byteBuffer.remaining()), funnel);
    return new BloomFilterIdSet(funnelType, bloomFilter, serializedSizeInBytes);
  }

  @Override
  public int hashCode() {
    return 31 * _funnelType.hashCode() + _bloomFilter.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BloomFilterIdSet)) {
      return false;
    }
    BloomFilterIdSet that = (BloomFilterIdSet) o;
    return _funnelType == that._funnelType && _bloomFilter.equals(that._bloomFilter);
  }
}
