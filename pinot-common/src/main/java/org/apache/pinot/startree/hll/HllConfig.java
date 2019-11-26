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
package org.apache.pinot.startree.hll;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.BaseJsonConfig;
import org.apache.pinot.common.utils.JsonUtils;


/**
 * HllConfig is used at segment generation.
 *
 * If columnsToDeriveHllFields are specified and not empty,
 * segment builder will generate corresponding hll derived fields on the fly.
 */
@Deprecated // Not required for the new star-tree
public class HllConfig extends BaseJsonConfig {
  private int _hllLog2m = HllConstants.DEFAULT_LOG2M;
  private String _hllDeriveColumnSuffix = HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX;
  private Set<String> _columnsToDeriveHllFields = new HashSet<>();

  private transient Map<String, String> _derivedHllFieldToOriginMap;

  /**
   * HllConfig with default hll log2m. No Hll derived field is generated.
   */
  public HllConfig() {
  }

  /**
   * HllConfig with customized hll log2m. No Hll derived field is generated.
   * @param hllLog2m The log2m parameter defines the accuracy of the counter.
   *                 The larger the log2m the better the accuracy.
   *                 accuracy = 1.04/sqrt(2^log2m)
   */
  public HllConfig(int hllLog2m) {
    _hllLog2m = hllLog2m;
  }

  /**
   * Hll derived field generation is enabled when columnsToDeriveHllFields is not empty.
   * @param hllLog2m The log2m parameter defines the accuracy of the counter.
   *                 The larger the log2m the better the accuracy.
   *                 accuracy = 1.04/sqrt(2^log2m)
   * @param columnsToDeriveHllFields columns to generate hll index
   * @param hllDeriveColumnSuffix suffix of column used for hll index
   */
  public HllConfig(int hllLog2m, Set<String> columnsToDeriveHllFields, String hllDeriveColumnSuffix) {
    Preconditions.checkNotNull(columnsToDeriveHllFields, "ColumnsToDeriveHllFields should not be null.");
    Preconditions.checkNotNull(hllDeriveColumnSuffix, "HLL Derived Field Suffix should not be null.");
    _hllLog2m = hllLog2m;
    _hllDeriveColumnSuffix = hllDeriveColumnSuffix;
    _columnsToDeriveHllFields = columnsToDeriveHllFields;
  }

  public int getHllLog2m() {
    return _hllLog2m;
  }

  public void setHllLog2m(int hllLog2m) {
    _hllLog2m = hllLog2m;
  }

  public String getHllDeriveColumnSuffix() {
    return _hllDeriveColumnSuffix;
  }

  public void setHllDeriveColumnSuffix(String hllDeriveColumnSuffix) {
    _hllDeriveColumnSuffix = hllDeriveColumnSuffix;
  }

  public Set<String> getColumnsToDeriveHllFields() {
    return _columnsToDeriveHllFields;
  }

  public void setColumnsToDeriveHllFields(Set<String> columnsToDeriveHllFields) {
    _columnsToDeriveHllFields = columnsToDeriveHllFields;
  }

  /**
   * Hll derived field generation is enabled when columnsToDeriveHllFields is not empty.
   * @return
   */
  @JsonIgnore
  public boolean isEnableHllIndex() {
    return _columnsToDeriveHllFields.size() > 0;
  }

  @JsonIgnore
  public int getHllFieldSize() {
    return HllSizeUtils.getHllFieldSizeFromLog2m(_hllLog2m);
  }

  @JsonIgnore
  public Map<String, String> getDerivedHllFieldToOriginMap() {
    if (_derivedHllFieldToOriginMap == null) {
      _derivedHllFieldToOriginMap = new HashMap<>();
      for (String columnName : _columnsToDeriveHllFields) {
        _derivedHllFieldToOriginMap.put(columnName + _hllDeriveColumnSuffix, columnName);
      }
    }
    return _derivedHllFieldToOriginMap;
  }

  public static HllConfig fromJsonString(String jsonString)
      throws IOException {
    return JsonUtils.stringToObject(jsonString, HllConfig.class);
  }
}
