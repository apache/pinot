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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.DimensionFieldSpec;


/**
 * Configs related to the MAP index:
 */
public class MapIndexConfig extends IndexConfig {
  public static final MapIndexConfig DISABLED = new MapIndexConfig(true);

  private int _maxKeys = 100;
  private List<DimensionFieldSpec> _denseKeys;
  private boolean _dynamicallyCreateDenseKeys;

  public MapIndexConfig() {
    this(false);
  }

  public MapIndexConfig(Boolean disabled) {
    super(disabled);
    _dynamicallyCreateDenseKeys = true;
    _denseKeys = List.of();
  }

  @JsonCreator
  public MapIndexConfig(@JsonProperty("disabled") @Nullable Boolean disabled,
      @JsonProperty("maxKeys") @Nullable Integer maxKeys,
      @JsonProperty("denseKeys") @Nullable List<DimensionFieldSpec> denseKeys,
      @JsonProperty("dynamicallyCreateDenseKeys") @Nullable Boolean dynamicallyCreateDenseKeys
  ) {
    super(disabled);
    _maxKeys = maxKeys != null ? maxKeys : 50;
    _dynamicallyCreateDenseKeys = dynamicallyCreateDenseKeys != null ? dynamicallyCreateDenseKeys : false;

    if (denseKeys != null) {
      _denseKeys = denseKeys;
    } else {
      _denseKeys = new LinkedList<>();
    }
  }

  public int getMaxKeys() {
    return _maxKeys;
  }

  public void setMaxKeys(int maxKeys) {
    _maxKeys = maxKeys;
  }

  public List<DimensionFieldSpec> getDenseKeys() {
    return _denseKeys;
  }

  public void setDenseKeys(List<DimensionFieldSpec> denseKeys) {
    _denseKeys = denseKeys;
  }

  public void setDynamicallyCreateDenseKeys(boolean dynamicallyCreateDenseKeys) {
    _dynamicallyCreateDenseKeys = dynamicallyCreateDenseKeys;
  }

  public boolean getDynamicallyCreateDenseKeys() {
    return _dynamicallyCreateDenseKeys;
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
    MapIndexConfig config = (MapIndexConfig) o;
    return _maxKeys == config._maxKeys && _dynamicallyCreateDenseKeys == config._dynamicallyCreateDenseKeys
        && _denseKeys.equals(config._denseKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _dynamicallyCreateDenseKeys, _maxKeys, _denseKeys);
  }

  public static class KeyConfigEntry {
    final String _name;
    final String _dataType;

    @JsonCreator
    public KeyConfigEntry(@JsonProperty("name") String name, @JsonProperty("dataType") String dataType) {
      _name = name;
      _dataType = dataType;
    }
  }
}
