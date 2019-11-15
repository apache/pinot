/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.pinot.thirdeye.datasource.cache.metadata;

/**
 * The cache loader to load the meta data. Used in the {@link MetaDataCollectionCache} to periodically reload the cache.
 * @see MetaDataCollectionCache for more details
 * @param <T> the type for cached the meta data
 */
public interface MetadataCacheLoader<T> {
  /**
   * Load the meta data from data base
   * @return the cached meta data
   */
  T loadMetaData();
}
