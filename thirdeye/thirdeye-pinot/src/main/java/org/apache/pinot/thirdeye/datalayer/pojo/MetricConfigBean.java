/*
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

package org.apache.pinot.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;

import java.util.Map;
import java.util.Objects;
import java.util.Set;


@JsonIgnoreProperties(ignoreUnknown=true)
public class MetricConfigBean extends AbstractBean {

  public static double DEFAULT_THRESHOLD = 0.01;
  public static String DERIVED_METRIC_ID_PREFIX = "id";
  public static final String ALIAS_JOINER = "::";
  public static final String URL_TEMPLATE_START_TIME = "startTime";
  public static final String URL_TEMPLATE_END_TIME = "endTime";
  public static final MetricAggFunction DEFAULT_AGG_FUNCTION = MetricAggFunction.SUM;
  public static final MetricAggFunction DEFAULT_TDIGEST_AGG_FUNCTION = MetricAggFunction.PCT90;
  public static final String METRIC_PROPERTIES_SEPARATOR = ",";

  /**
   * Properties to set in metricProperties, in order to express a metric as a metricAsDimension case.
   *
   * eg: If a dataset has columns as follows:
   * counter_name - a column which stores the name of the metric being expressed in that row
   * counter_value - a column which stores the metric value corresponding to the counter_name in that row
   * If we are interested in a metric called records_processed,
   * it means we are interested in counter_value from rows which have counter_name=records_processed
   * Thus the metric definition would be
   * {
   *   name : "RecordsProcessed",
   *   dataset : "myDataset",
   *   dimensionAsMetric : true,
   *   metricProperties : {
   *     "METRIC_NAMES" : "records_processed",
   *     "METRIC_NAMES_COLUMNS" : "counter_name",
   *     "METRIC_VALUES_COLUMN" : "counter_value"
   *   }
   * }
   *
   * eg: If a dataset has columns as follows:
   * counter_name_primary - a column which stores the name of the metric being expressed in that row
   * counter_name_secondary - another column which stores the name of the metric being expressed in that row
   * counter_value - a column which stores the metric value corresponding to the counter_name_primary and counter_name_secondary in that row
   * If we are interested in a metric called primary=records_processed secondary=internal,
   * it means we are interested in counter_value from rows which have counter_name_primary=records_processed and counter_name_secondary=internal
   * Thus the metric definition would be
   * {
   *   name : "RecordsProcessedInternal",
   *   dataset : "myDataset",
   *   dimensionAsMetric : true,
   *   metricProperties : {
   *     "METRIC_NAMES" : "records_processed,internal",
   *     "METRIC_NAMES_COLUMNS" : "counter_name_primary,counter_name_secondary",
   *     "METRIC_VALUES_COLUMN" : "counter_value"
   *   }
   * }
   */
  public enum DimensionAsMetricProperties {
    /** The actual names of the metrics, comma separated */
    METRIC_NAMES,
    /** The columns in which to look for the metric names, comma separated */
    METRIC_NAMES_COLUMNS,
    /** The column from which to get the metric value */
    METRIC_VALUES_COLUMN
  }

  private String name;

  private String dataset;

  private String alias;

  private Set<String> tags;

  private MetricType datatype;

  private boolean derived = false;

  private String derivedMetricExpression;

  private MetricAggFunction defaultAggFunction = DEFAULT_AGG_FUNCTION;

  private Double rollupThreshold = DEFAULT_THRESHOLD;

  private boolean inverseMetric = false;

  private String cellSizeExpression;

  private boolean active = true;

  private Map<String, String> extSourceLinkInfo;

  private Map<String, String> extSourceLinkTimeGranularity;

  private Map<String, String> metricProperties = null;

  private boolean dimensionAsMetric = false;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public Set<String> getTags() {
    return tags;
  }

  public void setTags(Set<String> tags) {
    this.tags = tags;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public MetricType getDatatype() {
    return datatype;
  }

  public void setDatatype(MetricType datatype) {
    this.datatype = datatype;
  }

  public boolean isDerived() {
    return derived;
  }

  public void setDerived(boolean derived) {
    this.derived = derived;
  }

  public String getDerivedMetricExpression() {
    return derivedMetricExpression;
  }

  public void setDerivedMetricExpression(String derivedMetricExpression) {
    this.derivedMetricExpression = derivedMetricExpression;
  }

  public MetricAggFunction getDefaultAggFunction() {
    return defaultAggFunction;
  }

  public void setDefaultAggFunction(MetricAggFunction defaultAggFunction) {
    this.defaultAggFunction = defaultAggFunction;
  }

  public boolean isDimensionAsMetric() {
    return dimensionAsMetric;
  }

  public void setDimensionAsMetric(boolean dimensionAsMetric) {
    this.dimensionAsMetric = dimensionAsMetric;
  }

  public Double getRollupThreshold() {
    return rollupThreshold;
  }

  public void setRollupThreshold(Double rollupThreshold) {
    this.rollupThreshold = rollupThreshold;
  }

  public boolean isInverseMetric() {
    return inverseMetric;
  }

  public void setInverseMetric(boolean inverseMetric) {
    this.inverseMetric = inverseMetric;
  }

  public String getCellSizeExpression() {
    return cellSizeExpression;
  }

  public void setCellSizeExpression(String cellSizeExpression) {
    this.cellSizeExpression = cellSizeExpression;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public Map<String, String> getExtSourceLinkInfo() {
    return extSourceLinkInfo;
  }

  public void setExtSourceLinkInfo(Map<String, String> extSourceLinkInfo) {
    this.extSourceLinkInfo = extSourceLinkInfo;
  }

  public Map<String, String> getExtSourceLinkTimeGranularity() {
    return extSourceLinkTimeGranularity;
  }

  public void setExtSourceLinkTimeGranularity(Map<String, String> extSourceLinkTimeGranularity) {
    this.extSourceLinkTimeGranularity = extSourceLinkTimeGranularity;
  }

  public Map<String, String> getMetricProperties() {
    return metricProperties;
  }

  public void setMetricProperties(Map<String, String> metricProperties) {
    this.metricProperties = metricProperties;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MetricConfigBean)) {
      return false;
    }
    MetricConfigBean mc = (MetricConfigBean) o;
    return Objects.equals(getId(), mc.getId())
        && Objects.equals(name, mc.getName())
        && Objects.equals(dataset, mc.getDataset())
        && Objects.equals(alias, mc.getAlias())
        && Objects.equals(derived, mc.isDerived())
        && Objects.equals(derivedMetricExpression, mc.getDerivedMetricExpression())
        && Objects.equals(defaultAggFunction, mc.getDefaultAggFunction())
        && Objects.equals(dimensionAsMetric, mc.isDimensionAsMetric())
        && Objects.equals(rollupThreshold, mc.getRollupThreshold())
        && Objects.equals(inverseMetric, mc.isInverseMetric())
        && Objects.equals(cellSizeExpression, mc.getCellSizeExpression())
        && Objects.equals(active, mc.isActive())
        && Objects.equals(extSourceLinkInfo, mc.getExtSourceLinkInfo())
        && Objects.equals(metricProperties, mc.getMetricProperties());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), dataset, alias, derived, derivedMetricExpression, defaultAggFunction, rollupThreshold,
        inverseMetric, cellSizeExpression, active, extSourceLinkInfo, metricProperties);
  }
}
