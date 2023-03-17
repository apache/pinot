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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;


/**
 * This is the entry point of the Index SPI, containing all index types that can be used.
 *
 * A caller can get a specific index type by calling {@link #get(String)} with their id or list all indexes by invoking
 * {@link IndexService#getAllIndexes()}.
 *
 * In production, there should be a single instance of this class and that instance should be returned by
 * {@link #getInstance()}. By default, this instance will be created by calling {@link #fromServiceLoader()}, which
 * reads all ServiceLoader SPI services that implement {@link IndexPlugin}, adding all the {@link IndexType} that can be
 * found in that way. In case we need to change the default behavior, the static instance can be changed with
 * {@link #setInstance(IndexService)}.
 *
 * Thread safety: All methods in this class, including static ones, are thread safe.
 *
 * Note: This class offers a singleton interface, but callers are encouraged to receive a IndexService instance in their
 * constructor and use that instance instead of using the static {@link #getInstance()} method directly. This is
 * specially important when callers want to test the behavior when dealing with different index type sets.
 *
 */
@ThreadSafe
public class IndexService {

  private static volatile IndexService _instance = fromServiceLoader();

  private final Set<IndexType<?, ?, ?>> _allIndexes;
  private final Map<String, IndexType<?, ?, ?>> _allIndexesById;

  private IndexService(Set<IndexPlugin<?>> allPlugins) {
    ImmutableMap.Builder<String, IndexType<?, ?, ?>> builder = ImmutableMap.builder();

    for (IndexPlugin<?> plugin : allPlugins) {
      IndexType<?, ?, ?> indexType = plugin.getIndexType();
      builder.put(indexType.getId().toLowerCase(Locale.US), indexType);
    }
    _allIndexesById = builder.build();
    _allIndexes = ImmutableSet.copyOf(_allIndexesById.values());
  }

  /**
   * Get the static IndexService instance that Pinot should use.
   */
  public static IndexService getInstance() {
    return _instance;
  }

  /**
   * Sets the static IndexService.
   *
   * This is the instance that will be used by most of the code.
   */
  public static void setInstance(IndexService other) {
    _instance = other;
  }

  /**
   * Creates an IndexService by looking for all {@link IndexPlugin} defined as services by {@link ServiceLoader}.
   *
   * This is the default way to create an IndexService. In case more tuning is needed,
   * {@link IndexService#IndexService(Set)}} can be used.
   */
  public static IndexService fromServiceLoader() {
    Set<IndexPlugin<?>> pluginList = new HashSet<>();
    for (IndexPlugin indexPlugin : ServiceLoader.load(IndexPlugin.class)) {
      pluginList.add(indexPlugin);
    }
    return new IndexService(pluginList);
  }

  /**
   * Returns a set with all the index types stored by this index service.
   *
   * @return an immutable list with all index types known by this instance.
   */
  public Set<IndexType<?, ?, ?>> getAllIndexes() {
    return _allIndexes;
  }

  /**
   * Get the IndexType that is identified by the given index id.
   *
   * All IndexType references must be obtained using the {@link IndexService} class (directly or indirectly). Even
   * if the callers have a compile time dependency to the actual IndexType class. Otherwise problems may arise when
   * using index overriding.
   *
   * In order to have a typesafe access to standard indexes, it is recommended to use {@link StandardIndexes} whenever
   * it is possible.
   *
   * @param indexId the index id, as defined in {@link IndexType#getId()}.
   * @return An optional which will be empty in case there is no index identified by that id.
   */
  public Optional<IndexType<?, ?, ?>> getOptional(String indexId) {
    return Optional.ofNullable(_allIndexesById.get(indexId.toLowerCase(Locale.US)));
  }

  /**
   * Get the IndexType that is identified by the given index id.
   *
   * All IndexType references must be obtained using the {@link IndexService} class (directly or indirectly). Even
   * if the callers have a compile time dependency to the actual IndexType class. Otherwise problems may arise when
   * using index overriding.
   *
   * In order to have a typesafe access to standard indexes, it is recommended to use {@link StandardIndexes} whenever
   * it is possible.
   *
   * @param indexId the index id, as defined in {@link IndexType#getId()}.
   * @return The index type that is identified by the given id.
   * @throws IllegalArgumentException in case there is not index type identified by the given id.
   */
  public IndexType<?, ?, ?> get(String indexId) {
    IndexType<?, ?, ?> indexType = _allIndexesById.get(indexId.toLowerCase(Locale.US));
    if (indexType == null) {
      throw new IllegalArgumentException("Unknown index id: " + indexId);
    }
    return indexType;
  }
}
