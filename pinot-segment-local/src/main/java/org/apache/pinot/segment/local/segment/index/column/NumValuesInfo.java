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
package org.apache.pinot.segment.local.segment.index.column;

public class NumValuesInfo {
  volatile int _numValues = 0;
  volatile int _maxNumValuesPerMVEntry = 0;

  public void updateSVEntry() {
    _numValues++;
  }

  public void updateMVEntry(int numValuesInMVEntry) {
    _numValues += numValuesInMVEntry;
    _maxNumValuesPerMVEntry = Math.max(_maxNumValuesPerMVEntry, numValuesInMVEntry);
  }

  public int getNumValues() {
    return _numValues;
  }

  public int getMaxNumValuesPerMVEntry() {
    return _maxNumValuesPerMVEntry;
  }
}
