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

import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.FSTIndexCreator;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.creator.GeoSpatialIndexCreator;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;


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

  public static IndexType<ForwardIndexConfig, ForwardIndexReader, ForwardIndexCreator> forward() {
    return (IndexType<ForwardIndexConfig, ForwardIndexReader, ForwardIndexCreator>)
        IndexService.getInstance().get(FORWARD_ID);
  }

  public static IndexType<DictionaryIndexConfig, Dictionary, ?> dictionary() {
    return (IndexType<DictionaryIndexConfig, Dictionary, ?>)
        IndexService.getInstance().get(DICTIONARY_ID);
  }

  public static IndexType<IndexConfig, NullValueVectorReader, ?> nullValueVector() {
    return (IndexType<IndexConfig, NullValueVectorReader, ?>)
        IndexService.getInstance().get(NULL_VALUE_VECTOR_ID);
  }

  public static IndexType<BloomFilterConfig, BloomFilterReader, BloomFilterCreator> bloomFilter() {
    return (IndexType<BloomFilterConfig, BloomFilterReader, BloomFilterCreator>)
        IndexService.getInstance().get(BLOOM_FILTER_ID);
  }

  public static IndexType<FstIndexConfig, TextIndexReader, FSTIndexCreator> fst() {
    return (IndexType<FstIndexConfig, TextIndexReader, FSTIndexCreator>)
        IndexService.getInstance().get(FST_ID);
  }

  public static IndexType<IndexConfig, InvertedIndexReader, DictionaryBasedInvertedIndexCreator> inverted() {
    return (IndexType<IndexConfig, InvertedIndexReader, DictionaryBasedInvertedIndexCreator>)
        IndexService.getInstance().get(INVERTED_ID);
  }

  public static IndexType<JsonIndexConfig, JsonIndexReader, JsonIndexCreator> json() {
    return (IndexType<JsonIndexConfig, JsonIndexReader, JsonIndexCreator>)
        IndexService.getInstance().get(JSON_ID);
  }

  public static IndexType<RangeIndexConfig, RangeIndexReader, CombinedInvertedIndexCreator> range() {
    return (IndexType<RangeIndexConfig, RangeIndexReader, CombinedInvertedIndexCreator>)
        IndexService.getInstance().get(RANGE_ID);
  }

  public static IndexType<TextIndexConfig, TextIndexReader, TextIndexCreator> text() {
    return (IndexType<TextIndexConfig, TextIndexReader, TextIndexCreator>)
        IndexService.getInstance().get(TEXT_ID);
  }

  public static IndexType<H3IndexConfig, H3IndexReader, GeoSpatialIndexCreator> h3() {
    return (IndexType<H3IndexConfig, H3IndexReader, GeoSpatialIndexCreator>)
        IndexService.getInstance().get(H3_ID);
  }
}
