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
package org.apache.pinot.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code ObjectSerDeUtils} class provides the utility methods to serialize/de-serialize objects.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ObjectSerDeUtils {
  private ObjectSerDeUtils() {
  }

  public static final int NULL_TYPE_VALUE = 100;

  /**
   * Serializer/De-serializer for a specific type of object.
   *
   * @param <T> Type of the object
   */
  public interface ObjectSerDe<T> {

    /**
     * Serializes a value into a byte array.
     */
    byte[] serialize(T value);

    /**
     * De-serializes a value from a byte array.
     */
    T deserialize(byte[] bytes);

    /**
     * De-serializes a value from a byte buffer.
     */
    T deserialize(ByteBuffer byteBuffer);
  }

  public static final ObjectSerDe<RoaringBitmap> ROARING_BITMAP_SER_DE = new ObjectSerDe<RoaringBitmap>() {

    @Override
    public byte[] serialize(RoaringBitmap bitmap) {
      byte[] bytes = new byte[bitmap.serializedSizeInBytes()];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      bitmap.serialize(byteBuffer);
      return bytes;
    }

    @Override
    public RoaringBitmap deserialize(byte[] bytes) {
      return deserialize(ByteBuffer.wrap(bytes));
    }

    @Override
    public RoaringBitmap deserialize(ByteBuffer byteBuffer) {
      RoaringBitmap bitmap = new RoaringBitmap();
      try {
        bitmap.deserialize(byteBuffer);
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while deserializing RoaringBitmap", e);
      }
      return bitmap;
    }
  };
}
