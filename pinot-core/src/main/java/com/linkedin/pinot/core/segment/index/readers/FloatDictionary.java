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

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;


/**
 * Nov 14, 2014
 */

public class FloatDictionary extends ImmutableDictionaryReader {

  public FloatDictionary(File dictFile, ColumnMetadata metadata, ReadMode loadMode) throws IOException {
    super(dictFile, metadata.getCardinality(), Float.SIZE / 8, loadMode == ReadMode.mmap);
  }

  @Override
  public int indexOf(Object rawValue) {
    Float lookup ;

    if (rawValue instanceof String) {
      lookup = new Float(Float.parseFloat((String)rawValue));
    } else {
      lookup = (Float) rawValue;
    }
    return floatIndexOf(lookup.floatValue());
  }

  @Override
  public Float get(int dictionaryId) {
    return new Float(getFloat(dictionaryId));
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return new Float(getFloat(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return new Double(getFloat(dictionaryId));
  }

  @Override
  public String toString(int dictionaryId) {
    return new Float(getFloat(dictionaryId)).toString();
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return new Float(getFloat(dictionaryId)).toString();
  }

  public float getFloat(int dictionaryId) {
    return dataFileReader.getFloat(dictionaryId, 0);
  }

  

}
