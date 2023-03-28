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
 *   IndexService.getInstance().get("bloom_filter")},
 *   a caller can simply use {@link StandardIndexes#bloomFilter()}
 * </p>
 */
@SuppressWarnings("unchecked")
public class StandardIndexes {
  public static final String FORWARD_ID = "forward_index";
  public static final String DICTIONARY_ID = "dictionary";
  public static final String NULL_VALUE_VECTOR_ID = "nullvalue_vector";
  public static final String BLOOM_FILTER_ID = "bloom_filter";
  public static final String FST_ID = "fst_index";
  public static final String INVERTED_ID = "inverted_index";
  public static final String JSON_ID = "json_index";
  public static final String RANGE_ID = "range_index";
  public static final String TEXT_ID = "text_index";
  public static final String H3_ID = "h3_index";

  private StandardIndexes() {
  }

  public static IndexType<?, ?, ?> forward() {
    return IndexService.getInstance().get(FORWARD_ID);
  }

  public static IndexType<?, ?, ?> dictionary() {
    return IndexService.getInstance().get(DICTIONARY_ID);
  }

  public static IndexType<?, ?, ?> nullValueVector() {
    return IndexService.getInstance().get(NULL_VALUE_VECTOR_ID);
  }

  public static IndexType<BloomFilterConfig, BloomFilterReader, IndexCreator> bloomFilter() {
    return (IndexType<BloomFilterConfig, BloomFilterReader, IndexCreator>)
        IndexService.getInstance().get(BLOOM_FILTER_ID);
  }

  public static IndexType<?, ?, ?> fst() {
    return IndexService.getInstance().get(FST_ID);
  }

  public static IndexType<?, ?, ?> inverted() {
    return IndexService.getInstance().get(INVERTED_ID);
  }

  public static IndexType<?, ?, ?> json() {
    return IndexService.getInstance().get(JSON_ID);
  }

  public static IndexType<?, ?, ?> range() {
    return IndexService.getInstance().get(RANGE_ID);
  }

  public static IndexType<?, ?, ?> text() {
    return IndexService.getInstance().get(TEXT_ID);
  }

  public static IndexType<?, ?, ?> h3() {
    return IndexService.getInstance().get(H3_ID);
  }
}
