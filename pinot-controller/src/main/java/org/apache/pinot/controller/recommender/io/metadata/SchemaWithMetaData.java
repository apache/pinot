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
package org.apache.pinot.controller.recommender.io.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;


/**
 * To accommodate the metadata piggyback on a standard pinot schmea,
 * See "schema" in SortedInvertedIndexInput.json
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaWithMetaData {

  @JsonProperty("dimensionFieldSpecs")
  private List<FieldMetadata> _dimensionFieldSpecs = new ArrayList<>();

  @JsonProperty("metricFieldSpecs")
  private List<FieldMetadata> _metricFieldSpecs = new ArrayList<>();

  @JsonProperty("dateTimeFieldSpecs")
  private List<DateTimeFieldSpecMetadata> _dateTimeFieldSpecs = new ArrayList<>();

  @JsonProperty("timeFieldSpec")
  private TimeFieldSpecMetadata _timeFieldSpec;

  public List<FieldMetadata> getDimensionFieldSpecs() {
    return _dimensionFieldSpecs;
  }

  public void setDimensionFieldSpecs(List<FieldMetadata> dimensionFieldSpecs) {
    _dimensionFieldSpecs = dimensionFieldSpecs;
  }

  public List<FieldMetadata> getMetricFieldSpecs() {
    return _metricFieldSpecs;
  }

  public void setMetricFieldSpecs(List<FieldMetadata> metricFieldSpecs) {
    _metricFieldSpecs = metricFieldSpecs;
  }

  public List<DateTimeFieldSpecMetadata> getDateTimeFieldSpecs() {
    return _dateTimeFieldSpecs;
  }

  public void setDateTimeFieldSpecs(List<DateTimeFieldSpecMetadata> dateTimeFieldSpecs) {
    _dateTimeFieldSpecs = dateTimeFieldSpecs;
  }

  public TimeFieldSpecMetadata getTimeFieldSpec() {
    return _timeFieldSpec;
  }

  public void setTimeFieldSpec(TimeFieldSpecMetadata timeFieldSpec) {
    _timeFieldSpec = timeFieldSpec;
  }
}
