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
package org.apache.pinot.segment.spi.index.mutable;

import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Interface for mutable dictionary (for CONSUMING segment).
 */
public interface MutableDictionary extends Dictionary {

  /**
   * Indexes a single-value entry (a value of the dictionary type) into the dictionary, and returns the dictId of the
   * value.
   */
  int index(Object value);

  /**
   * Indexes a multi-value entry (an array of values of the dictionary type) into the dictionary, and returns an array
   * of dictIds for each value.
   */
  int[] index(Object[] values);

  @Override
  default boolean isSorted() {
    return false;
  }

  @Override
  default int insertionIndexOf(String stringValue) {
    // This method should not be called for unsorted dictionary.
    throw new UnsupportedOperationException();
  }

  /**
   * This method returns a boolean denoting whether the mutable index can consume any more rows or not.
   */
  default boolean canAddMore() {
    return true;
  }
}
