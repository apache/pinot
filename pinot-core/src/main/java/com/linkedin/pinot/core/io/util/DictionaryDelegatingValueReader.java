/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.io.util;

import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.IOException;


/**
 * Value reader that delegates to a dictionary.
 */
public class DictionaryDelegatingValueReader implements ValueReader {
  private Dictionary _dictionary;

  public Dictionary getDictionary() {
    return _dictionary;
  }

  public void setDictionary(Dictionary dictionary) {
    _dictionary = dictionary;
  }

  @Override
  public int getInt(int index) {
    return _dictionary.getIntValue(index);
  }

  @Override
  public long getLong(int index) {
    return _dictionary.getLongValue(index);
  }

  @Override
  public float getFloat(int index) {
    return _dictionary.getFloatValue(index);
  }

  @Override
  public double getDouble(int index) {
    return _dictionary.getDoubleValue(index);
  }

  @Override
  public String getUnpaddedString(int index, int numBytesPerValue, byte paddingByte, byte[] buffer) {
    return _dictionary.getStringValue(index);
  }

  @Override
  public String getPaddedString(int index, int numBytesPerValue, byte[] buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes(int index, int numBytesPerValue, byte[] buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    _dictionary.close();
  }
}
