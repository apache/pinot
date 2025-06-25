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
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/*
 * TextIndexReader which allows querying a specific column within the index.
 */
public interface MultiColumnTextIndexReader extends TextIndexReader {

  /**
   * Returns the matching document ids for the given search query against given column.
   * This is the legacy method for backward compatibility.
   */
  default MutableRoaringBitmap getDocIds(String column, String searchQuery) {
    return getDocIds(searchQuery);
  }

  /**
   * Returns the matching document ids for the given search query against given column with options string.
   * Lucene-based multi-column text index readers should implement this method.
   * @param column The column name to search
   * @param searchQuery The search query string
   * @param optionsString Options string in format "key1=value1,key2=value2", can be null
   * @return Matching document ids
   */
  MutableRoaringBitmap getDocIds(String column, String searchQuery, @Nullable String optionsString);

  default boolean isMultiColumn() {
    return true;
  }
}
