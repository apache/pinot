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
package org.apache.pinot.core.realtime.impl;

import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Helper wrapper class for {@link MutableRoaringBitmap} to make it thread-safe.
 */
public class ThreadSafeMutableRoaringBitmap {
  private MutableRoaringBitmap _mutableRoaringBitmap;

  public ThreadSafeMutableRoaringBitmap() {
    _mutableRoaringBitmap = new MutableRoaringBitmap();
  }

  public ThreadSafeMutableRoaringBitmap(int firstDocId) {
    _mutableRoaringBitmap = new MutableRoaringBitmap();
    _mutableRoaringBitmap.add(firstDocId);
  }

  public void checkAndAdd(int docId) {
    if (!_mutableRoaringBitmap.contains(docId)) {
      synchronized (this) {
        _mutableRoaringBitmap.add(docId);
      }
    }
  }

  public void remove(int docId) {
    if(contains(docId)) {
      synchronized (this) {
        _mutableRoaringBitmap.remove(docId);
      }
    }
  }

  public boolean contains(int docId) {
    return _mutableRoaringBitmap.contains(docId);
  }

  public synchronized MutableRoaringBitmap getMutableRoaringBitmap() {
    return _mutableRoaringBitmap.clone();
  }
}
