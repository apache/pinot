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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;


/// Config for vector index.
///
/// Backend-neutral configuration for all vector index types. Common fields include `vectorIndexType`,
/// `vectorDimension`, `vectorDistanceFunction`, and `version`. All backend-specific configuration
/// (e.g., HNSW maxCon/beamWidth, IVF_FLAT nlist/trainSampleSize) is stored in the `properties` map.
///
/// The `vectorIndexType` field determines which backend is used. If absent or null, it defaults to
/// `HNSW` for backward compatibility. Use [#resolveBackendType()] to safely resolve the backend type
/// with this default applied.
///
/// @see VectorBackendType
/// @see VectorIndexConfigValidator
public class VectorIndexConfig extends IndexConfig {
  public static final VectorIndexConfig DISABLED = new VectorIndexConfig(true);
  private static final String VECTOR_INDEX_TYPE = "vectorIndexType";
  private static final String VECTOR_DIMENSION = "vectorDimension";
  private static final String VECTOR_DISTANCE_FUNCTION = "vectorDistanceFunction";
  private static final String VERSION = "version";
  private static final String DEFAULT_VERSION = "1";
  private static final VectorDistanceFunction DEFAULT_VECTOR_DISTANCE_FUNCTION =
      VectorDistanceFunction.COSINE;

  private String _vectorIndexType;
  private Integer _vectorDimension;
  private Integer _version;
  private VectorDistanceFunction _vectorDistanceFunction;
  private Map<String, String> _properties;

  /// @param disabled whether the config is disabled. Null is considered enabled.
  public VectorIndexConfig(Boolean disabled) {
    super(disabled);
    _properties = Map.of();
  }

  // Used to read from older configs
  public VectorIndexConfig(@Nullable Map<String, String> properties) {
    super(false);
    Preconditions.checkArgument(properties != null, "Properties must not be null");
    _vectorIndexType = properties.get(VECTOR_INDEX_TYPE);
    Preconditions.checkArgument(properties.containsKey(VECTOR_DIMENSION),
        "Properties must contain vector dimension");
    _vectorDimension = Integer.parseInt(properties.get(VECTOR_DIMENSION));
    _vectorDistanceFunction = properties.containsKey(VECTOR_DISTANCE_FUNCTION) ? VectorDistanceFunction.valueOf(
        properties.get(VECTOR_DISTANCE_FUNCTION)) : DEFAULT_VECTOR_DISTANCE_FUNCTION;
    _version = Integer.parseInt(properties.getOrDefault(VERSION, DEFAULT_VERSION));
    _properties = properties;
  }

  /// Binary-compat delegate for the pre-wrapper 6-arg primitive ctor. Kept so existing compiled call sites
  /// resolved to the primitive `(Boolean, String, int, int, VectorDistanceFunction, Map)` signature continue
  /// to link.
  @Deprecated
  public VectorIndexConfig(@Nullable Boolean disabled, @Nullable String vectorIndexType, int vectorDimension,
      int version, @Nullable VectorDistanceFunction vectorDistanceFunction,
      @Nullable Map<String, String> properties) {
    this(disabled, vectorIndexType, Integer.valueOf(vectorDimension), Integer.valueOf(version),
        vectorDistanceFunction, properties);
  }

  @JsonCreator
  public VectorIndexConfig(@JsonProperty("disabled") @Nullable Boolean disabled,
      @JsonProperty("vectorIndexType") @Nullable String vectorIndexType,
      @JsonProperty("vectorDimension") @Nullable Integer vectorDimension,
      @JsonProperty("version") @Nullable Integer version,
      @JsonProperty("vectorDistanceFunction") @Nullable VectorDistanceFunction vectorDistanceFunction,
      @JsonProperty("properties") @Nullable Map<String, String> properties) {
    super(disabled);
    _vectorIndexType = vectorIndexType;
    _vectorDimension = vectorDimension;
    _version = version;
    _vectorDistanceFunction = vectorDistanceFunction;
    _properties = properties != null ? properties : Map.of();
  }

  public String getVectorIndexType() {
    return _vectorIndexType;
  }

  public VectorIndexConfig setVectorIndexType(String vectorIndexType) {
    _vectorIndexType = vectorIndexType;
    return this;
  }

  public int getVectorDimension() {
    return _vectorDimension != null ? _vectorDimension : 0;
  }

  public VectorIndexConfig setVectorDimension(int vectorDimension) {
    _vectorDimension = vectorDimension;
    return this;
  }

  public VectorDistanceFunction getVectorDistanceFunction() {
    return _vectorDistanceFunction;
  }

  public VectorIndexConfig setVectorDistanceFunction(
      VectorDistanceFunction vectorDistanceFunction) {
    _vectorDistanceFunction = vectorDistanceFunction;
    return this;
  }

  public int getVersion() {
    return _version != null ? _version : 0;
  }

  public VectorIndexConfig setVersion(int version) {
    _version = version;
    return this;
  }

  public Map<String, String> getProperties() {
    return _properties;
  }

  public VectorIndexConfig setProperties(Map<String, String> properties) {
    _properties = properties != null ? properties : Map.of();
    return this;
  }

  /// Resolves the [VectorBackendType] for this config. If `vectorIndexType` is null or empty, defaults to
  /// [VectorBackendType#HNSW] for backward compatibility.
  ///
  /// @return the resolved backend type
  /// @throws IllegalArgumentException if vectorIndexType is set to an unrecognized value
  public VectorBackendType resolveBackendType() {
    return VectorIndexConfigValidator.resolveBackendType(this);
  }

  /// Curated slim serializer. See [IndexConfig#toJsonObject()] for the rationale.
  ///
  /// Each field is emitted only when explicitly configured (non-null wrapper). Empty `properties` map is
  /// treated as not-configured and omitted.
  @Override
  @JsonValue
  public ObjectNode toJsonObject() {
    ObjectNode node = super.toJsonObject();
    if (_vectorIndexType != null) {
      node.put("vectorIndexType", _vectorIndexType);
    }
    if (_vectorDimension != null) {
      node.put("vectorDimension", _vectorDimension);
    }
    if (_version != null) {
      node.put("version", _version);
    }
    if (_vectorDistanceFunction != null) {
      node.put("vectorDistanceFunction", _vectorDistanceFunction.name());
    }
    if (_properties != null && !_properties.isEmpty()) {
      node.set("properties", JsonUtils.objectToJsonNode(_properties));
    }
    return node;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    VectorIndexConfig that = (VectorIndexConfig) o;
    return Objects.equals(_vectorIndexType, that._vectorIndexType)
        && Objects.equals(_vectorDimension, that._vectorDimension)
        && Objects.equals(_version, that._version)
        && _vectorDistanceFunction == that._vectorDistanceFunction
        && Objects.equals(_properties, that._properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _vectorIndexType, _vectorDimension, _version, _vectorDistanceFunction,
        _properties);
  }

  public String toString() {
    return "VectorIndexConfig{" + "_vectorIndexType='" + _vectorIndexType + "', _vectorDimension="
        + _vectorDimension + ", _version=" + _version + ", _vectorDistanceFunction="
        + _vectorDistanceFunction + ", _properties=" + _properties + '}';
  }

  /// Distance functions supported by vector indexes.
  ///
  /// Note: `L2` is an alias for `EUCLIDEAN`. Both refer to Euclidean (L2) distance. Existing configs
  /// using `EUCLIDEAN` continue to work unchanged.
  public enum VectorDistanceFunction {
    COSINE, INNER_PRODUCT, EUCLIDEAN, DOT_PRODUCT, L2;
  }
}
