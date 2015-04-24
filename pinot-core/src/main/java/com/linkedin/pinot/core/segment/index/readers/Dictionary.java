/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

public interface Dictionary {

  public static final int DEFAULT_NULL_INT_VALUE = 0;
  public static final long DEFAULT_NULL_LONG_VALUE = 0L;
  public static final float DEFAULT_NULL_FLOAT_VALUE = 0F;
  public static final double DEFAULT_NULL_DOUBLE_VALUE = 0D;
  public static final String DEFAULT_NULL_STRING_VALUE = "null";

  public static final int NULL_VALUE_INDEX = -1;

  int indexOf(Object rawValue);

  Object get(int dictionaryId);

  long getLongValue(int dictionaryId);

  double getDoubleValue(int dictionaryId);

  String getStringValue(int dictionaryId);

  String toString(int dictionaryId);

  int length();
}
