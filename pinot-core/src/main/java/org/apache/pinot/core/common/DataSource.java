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
package org.apache.pinot.core.common;

import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.segment.index.readers.*;


public abstract class DataSource extends BaseOperator {

  /**
   * Returns the metadata for the data source.
   */
  public abstract DataSourceMetadata getDataSourceMetadata();

  /**
   * Returns the dictionary for the data source if the data is dictionary-encoded, or {@code null} if not.
   */
  public abstract Dictionary getDictionary();

  /**
   * Returns the inverted index for the data source if exists, or {@code null} if not.
   */
  public abstract InvertedIndexReader getInvertedIndex();

  /**
   * Returns the bloom filter for the data source if exists, or {@code null} if not.
   */
  public abstract BloomFilterReader getBloomFilter();

  public abstract PresenceVectorReader getPresenceVector();

}
