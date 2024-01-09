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
package org.apache.pinot.segment.local.utils;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class SegmentLocks {
  private static final SegmentLocks DEFAULT_LOCKS = create();
  private static final int DEFAULT_NUM_LOCKS = 10000;
  private final Lock[] _locks;
  private final int _numLocks;

  private SegmentLocks(int numLocks) {
    _numLocks = numLocks;
    _locks = new Lock[numLocks];
    for (int i = 0; i < numLocks; i++) {
      _locks[i] = new ReentrantLock();
    }
  }

  public Lock getLock(String tableNameWithType, String segmentName) {
    return _locks[Math.abs((31 * tableNameWithType.hashCode() + segmentName.hashCode()) % _numLocks)];
  }

  public static Lock getSegmentLock(String tableNameWithType, String segmentName) {
    return DEFAULT_LOCKS.getLock(tableNameWithType, segmentName);
  }

  public static SegmentLocks create() {
    return new SegmentLocks(DEFAULT_NUM_LOCKS);
  }

  public static SegmentLocks create(int numLocks) {
    return new SegmentLocks(numLocks);
  }
}
