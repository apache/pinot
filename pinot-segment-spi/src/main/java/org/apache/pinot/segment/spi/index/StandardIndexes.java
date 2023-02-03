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


public class StandardIndexes {
  private StandardIndexes() {
  }

  public static IndexType<ForwardIndexConfig, ForwardIndexReader, ForwardIndexCreator> forward() {
    return (IndexType<ForwardIndexConfig, ForwardIndexReader, ForwardIndexCreator>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("forward");
  }

  public static IndexType<DictionaryIndexConfig, Dictionary, ?> dictionary() {
    return (IndexType<DictionaryIndexConfig, Dictionary, ?>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("dictionary");
  }

  public static IndexType<IndexConfig, NullValueVectorReader, ?> nullValueVector() {
    return (IndexType<IndexConfig, NullValueVectorReader, ?>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("nullable");
  }

  public static IndexType<BloomFilterConfig, BloomFilterReader, BloomFilterCreator> bloomFilter() {
    return (IndexType<BloomFilterConfig, BloomFilterReader, BloomFilterCreator>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("bloom");
  }

  public static IndexType<FstIndexConfig, TextIndexReader, TextIndexCreator> fst() {
    return (IndexType<FstIndexConfig, TextIndexReader, TextIndexCreator>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("fst");
  }

  public static IndexType<IndexConfig, InvertedIndexReader, DictionaryBasedInvertedIndexCreator> inverted() {
    return (IndexType<IndexConfig, InvertedIndexReader, DictionaryBasedInvertedIndexCreator>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("inverted");
  }

  public static IndexType<JsonIndexConfig, JsonIndexReader, JsonIndexCreator> json() {
    return (IndexType<JsonIndexConfig, JsonIndexReader, JsonIndexCreator>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("json");
  }

  public static IndexType<RangeIndexConfig, RangeIndexReader, CombinedInvertedIndexCreator> range() {
    return (IndexType<RangeIndexConfig, RangeIndexReader, CombinedInvertedIndexCreator>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("range");
  }

  public static IndexType<TextIndexConfig, TextIndexReader, TextIndexCreator> text() {
    return (IndexType<TextIndexConfig, TextIndexReader, TextIndexCreator>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("text");
  }

  public static IndexType<H3IndexConfig, H3IndexReader, GeoSpatialIndexCreator> h3() {
    return (IndexType<H3IndexConfig, H3IndexReader, GeoSpatialIndexCreator>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("h3");
  }
}
