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

import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;


/**
 * This is the entry point of the Index SPI.
 *
 * Ideally, if we used some kind of injection system, this class should be injected into a Pinot context all classes can
 * receive when they are built. Given that Pinot doesn't have that, we have to relay on static fields.
 *
 * By default, this class will be initialized by reading all ServiceLoader SPI services that implement
 * {@link IndexPlugin}, adding all the {@link IndexType} that can be found in that way.
 *
 * In case we want to change the instance to be used at runtime, the method {@link #setInstance(IndexService)} can be
 * called.
 */
public class IndexService {

  private static volatile IndexService _instance = fromServiceLoader();

  private final Set<IndexType<?, ?, ?>> _allIndexes;

  public IndexService(Set<IndexPlugin<?>> allPlugins) {
    ImmutableSet.Builder<IndexType<?, ?, ?>> builder = ImmutableSet.builder();

    for (IndexPlugin<?> plugin : allPlugins) {
      builder.add(plugin.getIndexType());
    }
    _allIndexes = builder.build();
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
  public Optional<IndexType<?, ?, ?>> get(String indexId) {
    return getAllIndexes().stream().filter(indexType -> indexType.getId().equalsIgnoreCase(indexId)).findAny();
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
  public IndexType<?, ?, ?> getOrThrow(String indexId) {
    return get(indexId)
        .orElseThrow(() -> new IllegalArgumentException("Unknown index id: " + indexId));
  }
}
