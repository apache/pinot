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
package org.apache.pinot.segment.spi.index.reader;

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public interface TextIndexReader extends IndexReader {
  /**
   * Returns the matching dictionary ids for the given search query (optional).
   */
  ImmutableRoaringBitmap getDictIds(String searchQuery);

  /**
   * Returns the matching document ids for the given search query.
   * This is the legacy method for backward compatibility with native/FST text index readers.
   */
  MutableRoaringBitmap getDocIds(String searchQuery);

  /**
   * Returns the matching document ids for the given search query with options string.
   * This method allows passing options as a string parameter that will be parsed internally.
   * Lucene-based text index readers should implement this method.
   * @param searchQuery The search query string
   * @param optionsString Options string in format "key1=value1,key2=value2", can be null
   * @return Matching document ids
   */
  default MutableRoaringBitmap getDocIds(String searchQuery, @Nullable String optionsString) {
    // Default implementation falls back to the regular method for backward compatibility
    return getDocIds(searchQuery);
  }

  /**
   * Marker method that allows to differentiate between single-column and multi-column text index reader .
   */
  default boolean isMultiColumn() {
    return false;
  }
}
