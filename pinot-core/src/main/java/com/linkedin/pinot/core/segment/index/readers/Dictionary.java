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

  int DEFAULT_NULL_INT_VALUE = 0;
  long DEFAULT_NULL_LONG_VALUE = 0L;
  float DEFAULT_NULL_FLOAT_VALUE = 0F;
  double DEFAULT_NULL_DOUBLE_VALUE = 0D;
  String DEFAULT_NULL_STRING_VALUE = "null";

  int NULL_VALUE_INDEX = -1;

  int indexOf(Object rawValue);

  Object get(int dictionaryId);

  long getLongValue(int dictionaryId);

  double getDoubleValue(int dictionaryId);

  String getStringValue(int dictionaryId);

  float getFloatValue(int dictionaryId);

  int getIntValue(int dictionaryId);

  int length();

  /**
   * Batch API to read values corresponding to the input set of dictionary Ids.
   * Caller should ensure that outValues array size is atleast outStartPos + limit
   * @param dictionaryIds input dictionary ids for which to read values
   * @param startPos starting index in dictionaryIds
   * @param limit number of dictionary Ids to read from startPos
   * @param outValues array containing values corresponding to dictionaryIds
   *                  OutValues should be an array of appropriate type.
   *                  Example: String[] for StringDictionary implementation
   *                  double[] for DoubleDictionary. Use POD types where possible
   * @param outStartPos starting position in outValues. Values are copied starting
   *                    at this position.
   */
  void readIntValues(int[] dictionaryIds, int startPos, int limit, int[] outValues, int outStartPos);
  void readLongValues(int[] dictionaryIds, int startPos, int limit, long[] outValues, int outStartPos);
  void readDoubleValues(int[] dictionaryIds, int startPos, int limit, double[] outValues, int outStartPos);
  void readFloatValues(int[] dictionaryIds, int startPos, int limit, float[] outValues, int outStartPos);
  void readStringValues(int[] dictionaryIds, int startPos, int limit, String[] outValues, int outStartPos);
}
