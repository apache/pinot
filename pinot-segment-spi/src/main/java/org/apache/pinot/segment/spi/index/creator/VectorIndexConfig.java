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
package org.apache.pinot.segment.spi.index.creator;

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.IndexConfig;


/**
 * Config for vector index. Since this is generic configs for vector index, the only common fields are version,
 * vectorIndexType and vectorDimension. All the special configs for different vector index types should be put in
 * properties.
 */
public class VectorIndexConfig extends IndexConfig {
  public static final VectorIndexConfig DISABLED = new VectorIndexConfig(true);
  private static final String VECTOR_INDEX_TYPE = "vectorIndexType";
  private static final String VECTOR_DIMENSION = "vectorDimension";
  private static final String VERSION = "version";
  private static final String DEFAULT_VERSION = "1";

  private String _vectorIndexType;
  private int _vectorDimension;
  private int _version;
  private Map<String, String> _properties;

  /**
   * @param disabled whether the config is disabled. Null is considered enabled.
   */
  public VectorIndexConfig(Boolean disabled) {
    super(disabled);
  }

  // Used to read from older configs
  public VectorIndexConfig(@Nullable Map<String, String> properties) {
    super(false);
    Preconditions.checkArgument(properties != null, "Properties must not be null");
    Preconditions.checkArgument(properties.containsKey(VECTOR_INDEX_TYPE),
        "Properties must contain vector index type");
    _vectorIndexType = properties.get(VECTOR_INDEX_TYPE);
    Preconditions.checkArgument(properties.containsKey(VECTOR_DIMENSION),
        "Properties must contain vector dimension");
    _vectorDimension = Integer.parseInt(properties.get(VECTOR_DIMENSION));
    _version = Integer.parseInt(properties.getOrDefault(VERSION, DEFAULT_VERSION));
    _properties = properties;
  }

  public String getVectorIndexType() {
    return _vectorIndexType;
  }

  public VectorIndexConfig setVectorIndexType(String vectorIndexType) {
    _vectorIndexType = vectorIndexType;
    return this;
  }

  public int getVectorDimension() {
    return _vectorDimension;
  }

  public VectorIndexConfig setVectorDimension(int vectorDimension) {
    _vectorDimension = vectorDimension;
    return this;
  }

  public int getVersion() {
    return _version;
  }

  public VectorIndexConfig setVersion(int version) {
    _version = version;
    return this;
  }

  public Map<String, String> getProperties() {
    return _properties;
  }

  public VectorIndexConfig setProperties(Map<String, String> properties) {
    _properties = properties;
    return this;
  }
}
