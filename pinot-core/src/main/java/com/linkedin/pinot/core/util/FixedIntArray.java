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
package com.linkedin.pinot.core.util;

import java.util.Arrays;


/**
 * Wrapper around fixed size primitive int array. Provides the following so it can be used
 * as key in Maps/Sets.
 *
 * <ul>
 *   <li> hashCode()</li>
 *   <li> equals</li>
 * </ul>
 *
 * Note, does not provide a deep-copy of the value, and caller is responsible for maintaining the values.
 */
public class FixedIntArray {
  private final int[] _value;

  public FixedIntArray(int[] value) {
    _value = value;
  }

  public int[] elements() {
    return _value;
  }

  public int size() {
    return _value.length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FixedIntArray that = (FixedIntArray) o;

    return Arrays.equals(_value, that._value);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_value);
  }
}
