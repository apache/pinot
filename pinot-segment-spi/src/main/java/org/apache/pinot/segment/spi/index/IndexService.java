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

import com.google.common.collect.Sets;
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
    _allIndexes = Sets.newHashSetWithExpectedSize(allPlugins.size());

    for (IndexPlugin<?> plugin : allPlugins) {
      _allIndexes.add(plugin.getIndexType());
    }
  }

  public static IndexService getInstance() {
    return _instance;
  }

  public static void setInstance(IndexService other) {
    _instance = other;
  }

  public static IndexService fromServiceLoader() {
    Set<IndexPlugin<?>> pluginList = new HashSet<>();
    for (IndexPlugin indexPlugin : ServiceLoader.load(IndexPlugin.class)) {
      pluginList.add(indexPlugin);
    }
    return new IndexService(pluginList);
  }

  public Set<IndexType<?, ?, ?>> getAllIndexes() {
    return _allIndexes;
  }

  public Optional<IndexType<?, ?, ?>> getIndexTypeById(String indexId) {
    return getAllIndexes().stream().filter(indexType -> indexType.getId().equalsIgnoreCase(indexId)).findAny();
  }

  public IndexType<?, ?, ?> getIndexTypeByIdOrThrow(String indexId) {
    return getIndexTypeById(indexId)
        .orElseThrow(() -> new IllegalArgumentException("Unknown index id: " + indexId));
  }
}
