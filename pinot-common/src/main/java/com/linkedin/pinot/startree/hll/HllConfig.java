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
package com.linkedin.pinot.startree.hll;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.annotate.JsonIgnore;


/**
 * HllConfig is used at segment generation.
 *
 * If columnsToDeriveHllFields are specified and not empty,
 * segment builder will generate corresponding hll derived fields on the fly.
 */
public class HllConfig {
  private static final ImmutableBiMap<Integer, Integer> LOG2M_TO_SIZE_IN_BYTES =
      ImmutableBiMap.of(5, 32, 6, 52, 7, 96, 8, 180, 9, 352);

  private int hllLog2m = HllConstants.DEFAULT_LOG2M;
  private int hllFieldSize = getHllFieldSizeFromLog2m(HllConstants.DEFAULT_LOG2M);
  private String hllDeriveColumnSuffix = HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX;
  private Set<String> columnsToDeriveHllFields = new HashSet<>();

  private transient Map<String, String> derivedHllFieldToOriginMap;

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
    this.hllLog2m = hllLog2m;
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
    this.hllLog2m = hllLog2m;
    this.hllFieldSize = getHllFieldSizeFromLog2m(hllLog2m);
    this.hllDeriveColumnSuffix = hllDeriveColumnSuffix;
    this.columnsToDeriveHllFields = columnsToDeriveHllFields;
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

  /**
   * Hll derived field generation is enabled when columnsToDeriveHllFields is not empty.
   * @return
   */
  @JsonIgnore
  public boolean isEnableHllIndex() {
    return columnsToDeriveHllFields.size() > 0;
  }

  @JsonIgnore
  public int getHllFieldSize() {
    return hllFieldSize;
  }

  @JsonIgnore
  public Map<String, String> getDerivedHllFieldToOriginMap() {
    if (derivedHllFieldToOriginMap == null) {
      derivedHllFieldToOriginMap = new HashMap<>();
      for (String columnName : columnsToDeriveHllFields) {
        derivedHllFieldToOriginMap.put(columnName + hllDeriveColumnSuffix, columnName);
      }
    }
    return derivedHllFieldToOriginMap;
  }

  public static int getHllFieldSizeFromLog2m(int log2m) {
    Preconditions.checkArgument(LOG2M_TO_SIZE_IN_BYTES.containsKey(log2m),
        "Log2m: " + log2m + " is not in valid range.");
    return LOG2M_TO_SIZE_IN_BYTES.get(log2m);
  }
}
