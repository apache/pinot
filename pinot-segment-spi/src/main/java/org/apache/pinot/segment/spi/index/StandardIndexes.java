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

package org.apache.pinot.segment.spi.index;

import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.spi.config.table.BloomFilterConfig;


/**
 * This is a utility class that can be used to get references to the standard index types in a safe way.
 * <p>
 *   The ultimate container of valid index types (and therefore the source of truth) is {@link IndexService}.
 *   In order to get some specific index type, callers have to use {@link IndexService#get(String)}.
 *   The usability of this method is not great, given that the caller needs to use a literal (which imply that typos
 *   may produce exceptions at runtime) and it returns the most generic IndexType signature. This signature is so
 *   general that most of the time caller is forced to do castings.
 * </p>
 * <p>
 *   These usability problems are a cost we have to pay in order to have the ability to get references to index types
 *   that are not know at compile time, which is a requirement when we have custom index types. But Pinot includes
 *   a bunch of predefined index types (like forward index, dictionary, null value vector, etc) that should always be
 *   included in a Pinot distribution. {@link StandardIndexes} contains one get method for each standard index,
 *   providing a typesafe way to get these references. Instead of having to write something like
 *   {@code (IndexType<BloomFilterConfig, BloomFilterReader, IndexCreator>)
 *   IndexService.getInstance().getOrThrow("bloom_filter")},
 *   a caller can simply use {@link StandardIndexes#bloomFilter()}
 * </p>
 */
@SuppressWarnings("unchecked")
public class StandardIndexes {
  private StandardIndexes() {
  }

  // Other methods like bloomFilter() should be created for each index.
  // This class may be changed in the future by adding a way to override index implementations in needed, like
  // current IndexOverrides

  public static IndexType<BloomFilterConfig, BloomFilterReader, IndexCreator> bloomFilter() {
    return (IndexType<BloomFilterConfig, BloomFilterReader, IndexCreator>)
        IndexService.getInstance().get("bloom_filter");
  }
}
