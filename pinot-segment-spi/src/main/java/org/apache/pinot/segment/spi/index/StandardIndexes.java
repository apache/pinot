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


@SuppressWarnings("unchecked")
public class StandardIndexes {
  private StandardIndexes() {
  }

  public static IndexType<?, ?, ?> forward() {
    return IndexService.getInstance().getOrThrow("forward_index");
  }

  public static IndexType<?, ?, ?> dictionary() {
    return IndexService.getInstance().getOrThrow("dictionary");
  }

  public static IndexType<?, ?, ?> nullValueVector() {
    return IndexService.getInstance().getOrThrow("nullvalue_vector");
  }

  public static IndexType<BloomFilterConfig, BloomFilterReader, IndexCreator> bloomFilter() {
    return (IndexType<BloomFilterConfig, BloomFilterReader, IndexCreator>)
        IndexService.getInstance().getOrThrow("bloom_filter");
  }

  public static IndexType<?, ?, ?> fst() {
    return IndexService.getInstance().getOrThrow("fst_index");
  }

  public static IndexType<?, ?, ?> inverted() {
    return IndexService.getInstance().getOrThrow("inverted_index");
  }

  public static IndexType<?, ?, ?> json() {
    return IndexService.getInstance().getOrThrow("json_index");
  }

  public static IndexType<?, ?, ?> range() {
    return IndexService.getInstance().getOrThrow("range_index");
  }

  public static IndexType<?, ?, ?> text() {
    return IndexService.getInstance().getOrThrow("text_index");
  }

  public static IndexType<?, ?, ?> h3() {
    return IndexService.getInstance().getOrThrow("h3_index");
  }
}
