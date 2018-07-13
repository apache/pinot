/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.ConfigKey;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.pinot.common.utils.EqualityUtils.hashCodeOf;
import static com.linkedin.pinot.common.utils.EqualityUtils.isEqual;
import static com.linkedin.pinot.common.utils.EqualityUtils.isNullOrNotSameClass;
import static com.linkedin.pinot.common.utils.EqualityUtils.isSameReference;


/**
 * HllConfig is used at segment generation.
 *
 * If columnsToDeriveHllFields are specified and not empty,
 * segment builder will generate corresponding hll derived fields on the fly.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HllConfig {
  @ConfigKey("hllLog2m")
  private int hllLog2m = HllConstants.DEFAULT_LOG2M;

  @ConfigKey("hllFieldSize")
  private int hllFieldSize = HllSizeUtils.getHllFieldSizeFromLog2m(HllConstants.DEFAULT_LOG2M);

  @ConfigKey("hllDeriveColumnSuffix")
  private String hllDeriveColumnSuffix = HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX;

  private Set<String> columnsToDeriveHllFields = new HashSet<>();

  private transient Map<String, String> derivedHllFieldToOriginMap;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
    this.hllFieldSize = HllSizeUtils.getHllFieldSizeFromLog2m(hllLog2m);
    this.hllDeriveColumnSuffix = hllDeriveColumnSuffix;
    this.columnsToDeriveHllFields = columnsToDeriveHllFields;
  }

  public int getHllLog2m() {
    return hllLog2m;
  }

  public void setHllLog2m(int hllLog2m) {
    this.hllLog2m = hllLog2m;
    this.hllFieldSize = HllSizeUtils.getHllFieldSizeFromLog2m(hllLog2m);
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

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

  public String toJsonString() throws Exception {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public static HllConfig fromJsonString(String jsonString) throws IOException {
    return OBJECT_MAPPER.readValue(jsonString, HllConfig.class);
  }

  @Override
  public boolean equals(Object o) {
    if (isSameReference(this, o)) {
      return true;
    }

    if (isNullOrNotSameClass(this, o)) {
      return false;
    }

    HllConfig hllConfig = (HllConfig) o;

    return isEqual(hllLog2m, hllConfig.hllLog2m) &&
        isEqual(hllFieldSize, hllConfig.hllFieldSize) &&
        isEqual(hllDeriveColumnSuffix, hllConfig.hllDeriveColumnSuffix) &&
        isEqual(columnsToDeriveHllFields, hllConfig.columnsToDeriveHllFields) &&
        isEqual(derivedHllFieldToOriginMap, hllConfig.derivedHllFieldToOriginMap);
  }

  @Override
  public int hashCode() {
    int result = hashCodeOf(hllLog2m);
    result = hashCodeOf(result, hllFieldSize);
    result = hashCodeOf(result, hllDeriveColumnSuffix);
    result = hashCodeOf(result, columnsToDeriveHllFields);
    result = hashCodeOf(result, derivedHllFieldToOriginMap);
    return result;
  }
}
