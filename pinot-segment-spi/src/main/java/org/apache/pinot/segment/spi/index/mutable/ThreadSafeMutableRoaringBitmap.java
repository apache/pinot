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
package org.apache.pinot.segment.spi.index.mutable;

import java.nio.ByteBuffer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Helper wrapper class for {@link MutableRoaringBitmap} to make it thread-safe.
 */
public class ThreadSafeMutableRoaringBitmap {
  private final MutableRoaringBitmap _mutableRoaringBitmap;

  public ThreadSafeMutableRoaringBitmap() {
    _mutableRoaringBitmap = new MutableRoaringBitmap();
  }

  public ThreadSafeMutableRoaringBitmap(int firstDocId) {
    _mutableRoaringBitmap = new MutableRoaringBitmap();
    _mutableRoaringBitmap.add(firstDocId);
  }

  public ThreadSafeMutableRoaringBitmap(MutableRoaringBitmap mutableRoaringBitmap) {
    _mutableRoaringBitmap = mutableRoaringBitmap;
  }

  public synchronized void add(int docId) {
    _mutableRoaringBitmap.add(docId);
  }

  public synchronized boolean contains(int docId) {
    return _mutableRoaringBitmap.contains(docId);
  }

  public synchronized void remove(int docId) {
    _mutableRoaringBitmap.remove(docId);
  }

  public synchronized void replace(int oldDocId, int newDocId) {
    _mutableRoaringBitmap.remove(oldDocId);
    _mutableRoaringBitmap.add(newDocId);
  }

  public synchronized MutableRoaringBitmap getMutableRoaringBitmap() {
    return _mutableRoaringBitmap.clone();
  }

  /**
   * Returns a point-in-time snapshot of the bitmap as a serialized byte array.
   * This avoids cloning the full object clone() unlike in getMutableRoaringBitmap()
   */
  public synchronized byte[] toBytes() {
    ByteBuffer buf = ByteBuffer.allocate(_mutableRoaringBitmap.serializedSizeInBytes());
    _mutableRoaringBitmap.serialize(buf);
    return buf.array();
  }

  public synchronized int getCardinality() {
    return _mutableRoaringBitmap.getCardinality();
  }

  /**
   * Returns a consistent point-in-time snapshot containing both the serialized bytes and
   * cardinality captured under a single lock.
   */
  public synchronized DocIdSnapshot toSnapshot() {
    ByteBuffer buf = ByteBuffer.allocate(_mutableRoaringBitmap.serializedSizeInBytes());
    _mutableRoaringBitmap.serialize(buf);
    return new DocIdSnapshot(buf.array(), _mutableRoaringBitmap.getCardinality());
  }

  public static final class DocIdSnapshot {
    private final byte[] _bytes;
    private final int _cardinality;

    public DocIdSnapshot(byte[] bytes, int cardinality) {
      _bytes = bytes;
      _cardinality = cardinality;
    }

    public byte[] getBytes() {
      return _bytes;
    }

    public int getCardinality() {
      return _cardinality;
    }
  }

  public synchronized boolean isEmpty() {
    return _mutableRoaringBitmap.isEmpty();
  }
}
