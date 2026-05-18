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
package org.apache.pinot.segment.local.segment.index.readers;

import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/// Base class for constant-value (single-value) dictionaries.
///
/// Unlike [BaseImmutableDictionary] which is backed by a segment data buffer, constant-value dictionaries hold exactly
/// one value in memory and support the stats-collection APIs needed when sealing a consuming segment.
public abstract class BaseConstantValueDictionary implements Dictionary {

  @Override
  public boolean isSorted() {
    return true;
  }

  @Override
  public int length() {
    return 1;
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    // This method should not be called for sorted dictionary.
    throw new UnsupportedOperationException();
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return 0;
  }

  @Override
  public int getLengthOfShortestElement() {
    return getValueType().size();
  }

  @Override
  public int getLengthOfLongestElement() {
    return getValueType().size();
  }

  @Override
  public boolean isAscii() {
    return false;
  }

  @Override
  public void close()
      throws IOException {
  }
}
