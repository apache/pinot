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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.roaringbitmap.longlong.Roaring64NavigableMap;


/**
 * The {@code Roaring64NavigableMapIdSet} is an IdSet backed by the {@link Roaring64NavigableMap}, and can be used to
 * store LONG ids.
 */
public class Roaring64NavigableMapIdSet implements IdSet {
  private final Roaring64NavigableMap _bitmap;

  Roaring64NavigableMapIdSet() {
    _bitmap = new Roaring64NavigableMap();
  }

  private Roaring64NavigableMapIdSet(Roaring64NavigableMap bitmap) {
    _bitmap = bitmap;
  }

  Roaring64NavigableMap getBitmap() {
    return _bitmap;
  }

  @Override
  public Type getType() {
    return Type.ROARING_64_NAVIGABLE_MAP;
  }

  @Override
  public void add(long id) {
    _bitmap.addLong(id);
  }

  @Override
  public boolean contains(long id) {
    return _bitmap.contains(id);
  }

  @Override
  public int getSerializedSizeInBytes() {
    return 1 + (int) _bitmap.serializedSizeInBytes();
  }

  @Override
  public byte[] toBytes() throws IOException {
    int numBytes = 1 + (int) _bitmap.serializedSizeInBytes();
    // NOTE: No need to close these streams.
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(numBytes);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    dataOutputStream.write(Type.ROARING_64_NAVIGABLE_MAP.getId());
    _bitmap.serialize(dataOutputStream);
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Deserializes the Roaring64NavigableMapIdSet from a ByteBuffer.
   * <p>NOTE: The ByteBuffer does not include the IdSet.Type byte.
   */
  static Roaring64NavigableMapIdSet fromByteBuffer(ByteBuffer byteBuffer) throws IOException {
    Preconditions.checkArgument(byteBuffer.hasArray(),
        "Cannot deserialize Roaring64NavigableMap from ByteBuffer not backed by an accessible byte array");
    Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();
    // NOTE: No need to close these streams.
    roaring64NavigableMap.deserialize(new DataInputStream(new ByteArrayInputStream(byteBuffer.array(),
        byteBuffer.arrayOffset() + byteBuffer.position(), byteBuffer.remaining())));
    return new Roaring64NavigableMapIdSet(roaring64NavigableMap);
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
    if (!(o instanceof Roaring64NavigableMapIdSet)) {
      return false;
    }
    Roaring64NavigableMapIdSet that = (Roaring64NavigableMapIdSet) o;
    return _bitmap.equals(that._bitmap);
  }
}
