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
package com.linkedin.pinot.util.datasource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class IntArrayBackedImmutableDictionary extends ImmutableDictionaryReader {

  private final int[] dictionary;

  protected IntArrayBackedImmutableDictionary(File dictFile, int rows, int columnSize, boolean isMmap,
      final int[] dictionary) throws IOException {
    super(dictFile, rows, columnSize, isMmap);
    this.dictionary = dictionary;
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return String.valueOf(dictionary[dictionaryId]);
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return dictionary[dictionaryId];
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return dictionary[dictionaryId];
  }

  @Override
  public String toString(int dictionaryId) {
    return String.valueOf(dictionary[dictionaryId]);
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return Arrays.binarySearch(dictionary, Integer.parseInt(rawValue.toString()));
    }
    return Arrays.binarySearch(dictionary, ((Integer) rawValue).intValue());
  }

  @Override
  public Object get(int dictionaryId) {
    return new Integer(dictionary[dictionaryId]);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return dictionary[dictionaryId];
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return dictionary[dictionaryId];
  }
}
