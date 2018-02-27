/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.readers;

import java.io.Closeable;


public interface Dictionary extends Closeable {
  int NULL_VALUE_INDEX = -1;

  /**
   * Returns index of the object in the dictionary.
   * Returns -1, if the object does not exist in the dictionary.
   *
   * @param rawValue Object for which to find the index.
   * @return Index of object, or -1 if not found.
   */
  int indexOf(Object rawValue);

  Object get(int dictId);

  int getIntValue(int dictId);

  long getLongValue(int dictId);

  float getFloatValue(int dictId);

  double getDoubleValue(int dictId);

  String getStringValue(int dictId);

  int length();

  boolean isSorted();

  // Batch read APIs

  void readIntValues(int[] dictIds, int inStartPos, int length, int[] outValues, int outStartPos);

  void readLongValues(int[] dictIds, int inStartPos, int length, long[] outValues, int outStartPos);

  void readFloatValues(int[] dictIds, int inStartPos, int length, float[] outValues, int outStartPos);

  void readDoubleValues(int[] dictIds, int inStartPos, int length, double[] outValues, int outStartPos);

  void readStringValues(int[] dictIds, int inStartPos, int length, String[] outValues, int outStartPos);

}
