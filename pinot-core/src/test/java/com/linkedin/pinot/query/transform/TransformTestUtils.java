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
package com.linkedin.pinot.query.transform;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BaseBlockValSet;


/**
 * Utils class for transform related unit tests.
 */
public class TransformTestUtils {

  // Private constructor, so the class cannot be initialized.
  private TransformTestUtils() {

  }

  /**
   * Class for mocking {@link com.linkedin.pinot.core.common.BlockValSet}, for unit testing.
   * Callers need to ensure type safety at their end.
   */
  public static class TestBlockValSet extends BaseBlockValSet {
    private Object _values;
    private int _numDocs;
    private DataType _dataType;

    public TestBlockValSet(Object values, int numDocs, DataType dataType) {
      _values = values;
      _numDocs = numDocs;
      _dataType = dataType;
    }

    public TestBlockValSet(Object values, int numDocs) {
      _values = values;
      _numDocs = numDocs;
    }

    @Override
    public long[] getLongValuesSV() {
      return (long[]) _values;
    }

    @Override
    public double[] getDoubleValuesSV() {
      return (double[]) _values;
    }

    @Override
    public String[] getStringValuesSV() {
      return (String[]) _values;
    }

    @Override
    public DataType getValueType() {
      return _dataType;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }
  }
}
