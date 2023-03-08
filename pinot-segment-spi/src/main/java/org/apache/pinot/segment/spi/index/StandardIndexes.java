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

  // Other methods like bloomFilter() should be created for each index.
  // This class may be changed in the future by adding a way to override index implementations in needed, like
  // current IndexOverrides

  public static IndexType<BloomFilterConfig, BloomFilterReader, IndexCreator> bloomFilter() {
    return (IndexType<BloomFilterConfig, BloomFilterReader, IndexCreator>)
        IndexService.getInstance().getIndexTypeByIdOrThrow("bloom_filter");
  }
}
