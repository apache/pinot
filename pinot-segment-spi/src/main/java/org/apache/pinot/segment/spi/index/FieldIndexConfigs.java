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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * FieldIndexConfigs are a map like structure that relates index types with their configuration, providing a type safe
 * interface.
 */
public class FieldIndexConfigs {

  public static final FieldIndexConfigs EMPTY = new FieldIndexConfigs(new HashMap<>());
  private final Map<IndexType, IndexDeclaration<?>> _configMap;

  private FieldIndexConfigs(Map<IndexType, IndexDeclaration<?>> configMap) {
    _configMap = Collections.unmodifiableMap(configMap);
  }


  /**
   * The method most of the code should call to know how is configured a given index.
   *
   * @param indexType the type of the index we are interested in.
   * @return How the index has been configured, which can be {@link IndexDeclaration#isDeclared() not declared},
   * {@link IndexDeclaration#isEnabled()} not enabled} or an actual configuration object (which can be obtained with
   * {@link IndexDeclaration#getEnabledConfig()}.
   */
  public <C, I extends IndexType<C, ?, ?>> IndexDeclaration<C> getConfig(I indexType) {
    @SuppressWarnings("unchecked")
    IndexDeclaration<C> config = (IndexDeclaration<C>) _configMap.get(indexType);
    if (config == null) {
      return IndexDeclaration.notDeclared(indexType);
    }
    return config;
  }

  public static class Builder {
    private final Map<IndexType, IndexDeclaration<?>> _configMap;

    public Builder() {
      _configMap = new HashMap<>();
    }

    public Builder(FieldIndexConfigs other) {
      _configMap = new HashMap<>(other._configMap);
    }

    public <C, I extends IndexType<C, ?, ?>> Builder add(I indexType, @Nullable C config) {
      _configMap.put(indexType, IndexDeclaration.declared(config));
      return this;
    }

    public <C, I extends IndexType<C, ?, ?>> Builder addDeclaration(I indexType, IndexDeclaration<C> declaration) {
      if (!declaration.isDeclared()) {
        undeclare(indexType);
      } else {
        _configMap.put(indexType, declaration);
      }
      return this;
    }

    public Builder addUnsafe(IndexType<?, ?, ?> indexType, @Nullable Object config) {
      Preconditions.checkArgument(!(config instanceof IndexDeclaration), "Index declarations cannot be "
          + "added as values");
      _configMap.put(indexType, IndexDeclaration.declared(config));
      return this;
    }

    public Builder addUnsafeDeclaration(IndexType<?, ?, ?> indexType, @Nullable IndexDeclaration<?> config) {
      if (!config.isDeclared()) {
        undeclare(indexType);
      } else {
        _configMap.put(indexType, config);
      }
      return this;
    }

    public Builder undeclare(IndexType<?, ?, ?> indexType) {
      _configMap.remove(indexType);
      return this;
    }

    public FieldIndexConfigs build() {
      return new FieldIndexConfigs(_configMap);
    }
  }

  public static class UnrecognizedIndexException extends RuntimeException {
    private final String _indexId;

    public UnrecognizedIndexException(String indexId) {
      super("There is no index type whose identified as " + indexId);
      _indexId = indexId;
    }

    public String getIndexId() {
      return _indexId;
    }
  }
}
