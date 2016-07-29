/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree.hll;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.annotate.JsonIgnore;


/**
 * HllConfig is the config for hll index generation.
 * Note it is only effective when StarTree index generation is enabled.
 */
public class HllConfig {
  private boolean enableHllIndex = true;
  private int hllLog2m = HllConstants.DEFAULT_LOG2M;
  private int hllFieldSize = HllUtil.getHllFieldSizeFromLog2m(HllConstants.DEFAULT_LOG2M);
  private String hllDeriveColumnSuffix = HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX;
  private Set<String> columnsToDeriveHllFields = new HashSet<>();

  private transient Map<String, String> derivedHllFieldsMap;

  /**
   * HllConfig with empty columnsToDeriveHllFields, default hll log2m, default hll suffix.
   */
  public HllConfig() {
  }

  /**
   * HllConfig with default hll log2m, default hll suffix.
   * @param columnsToDeriveHllFields columns to generate hll index
   */
  public HllConfig(Set<String> columnsToDeriveHllFields) {
    this(columnsToDeriveHllFields, HllConstants.DEFAULT_LOG2M);
  }

  /**
   * HllConfig with default hll suffix.
   * @param columnsToDeriveHllFields columns to generate hll index
   * @param hllLog2m The log2m parameter defines the accuracy of the counter.
   *                 The larger the log2m the better the accuracy.
   *                 accuracy = 1.04/sqrt(2^log2m)
   */
  public HllConfig(Set<String> columnsToDeriveHllFields, int hllLog2m) {
    this(columnsToDeriveHllFields, hllLog2m, HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX);
  }

  /**
   *
   * @param columnsToDeriveHllFields columns to generate hll index
   * @param hllLog2m The log2m parameter defines the accuracy of the counter.
   *                 The larger the log2m the better the accuracy.
   *                 accuracy = 1.04/sqrt(2^log2m)
   * @param hllDeriveColumnSuffix suffix of column used for hll index
   */
  public HllConfig(Set<String> columnsToDeriveHllFields, int hllLog2m, String hllDeriveColumnSuffix) {
    Preconditions.checkNotNull(columnsToDeriveHllFields, "ColumnsToDeriveHllFields should not be null.");
    Preconditions.checkNotNull(hllDeriveColumnSuffix, "HLL Derived Field Suffix should not be null.");
    this.hllLog2m = hllLog2m;
    this.hllFieldSize = HllUtil.getHllFieldSizeFromLog2m(hllLog2m);
    this.columnsToDeriveHllFields = columnsToDeriveHllFields;
    this.hllDeriveColumnSuffix = hllDeriveColumnSuffix;
  }

  public boolean isEnableHllIndex() {
    return enableHllIndex;
  }

  public void setEnableHllIndex(boolean enableHllIndex) {
    this.enableHllIndex = enableHllIndex;
  }

  public int getHllLog2m() {
    return hllLog2m;
  }

  public void setHllLog2m(int hllLog2m) {
    this.hllLog2m = hllLog2m;
  }

  public String getHllDeriveColumnSuffix() {
    return hllDeriveColumnSuffix;
  }

  public void setHllDeriveColumnSuffix(String hllDeriveColumnSuffix) {
    this.hllDeriveColumnSuffix = hllDeriveColumnSuffix;
  }

  public Set<String> getColumnsToDeriveHllFields() {
    return columnsToDeriveHllFields;
  }

  public void setColumnsToDeriveHllFields(Set<String> columnsToDeriveHllFields) {
    this.columnsToDeriveHllFields = columnsToDeriveHllFields;
  }

  @JsonIgnore
  public int getHllFieldSize() {
    return hllFieldSize;
  }

  @JsonIgnore
  public Map<String, String> getDerivedHllFieldsMap() {
    if (derivedHllFieldsMap == null) {
      derivedHllFieldsMap = new HashMap<>();
      for (String columnName : columnsToDeriveHllFields) {
        derivedHllFieldsMap.put(columnName, columnName + hllDeriveColumnSuffix);
      }
    }
    return derivedHllFieldsMap;
  }
}
