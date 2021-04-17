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
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code RoaringBitmapIdSet} is an IdSet backed by the {@link RoaringBitmap}, and can be used to store INT ids.
 */
public class RoaringBitmapIdSet implements IdSet {
  private final RoaringBitmap _bitmap;

  RoaringBitmapIdSet() {
    _bitmap = new RoaringBitmap();
  }

  private RoaringBitmapIdSet(RoaringBitmap bitmap) {
    _bitmap = bitmap;
  }

  RoaringBitmap getBitmap() {
    return _bitmap;
  }

  @Override
  public Type getType() {
    return Type.ROARING_BITMAP;
  }

  @Override
  public void add(int id) {
    _bitmap.add(id);
  }

  @Override
  public boolean contains(int id) {
    return _bitmap.contains(id);
  }

  @Override
  public int getSerializedSizeInBytes() {
    return 1 + _bitmap.serializedSizeInBytes();
  }

  @Override
  public byte[] toBytes() {
    int numBytes = 1 + _bitmap.serializedSizeInBytes();
    byte[] bytes = new byte[numBytes];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.put(Type.ROARING_BITMAP.getId());
    _bitmap.serialize(byteBuffer);
    return bytes;
  }

  /**
   * Deserializes the RoaringBitmapIdSet from a ByteBuffer.
   * <p>NOTE: The ByteBuffer does not include the IdSet.Type byte.
   */
  static RoaringBitmapIdSet fromByteBuffer(ByteBuffer byteBuffer) throws IOException {
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.deserialize(byteBuffer);
    return new RoaringBitmapIdSet(roaringBitmap);
  }

  @Override
  public int hashCode() {
    return _bitmap.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RoaringBitmapIdSet)) {
      return false;
    }
    RoaringBitmapIdSet that = (RoaringBitmapIdSet) o;
    return _bitmap.equals(that._bitmap);
  }
}
