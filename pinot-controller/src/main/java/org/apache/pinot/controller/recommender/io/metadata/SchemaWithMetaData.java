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
import java.util.ArrayList;
import java.util.List;


/**
 * To accommodate the metadata piggyback on a standard pinot schmea,
 * See "schema" in SortedInvertedIndexInput.json
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaWithMetaData {
  private List<FieldMetadata> _dimensionFieldSpecs = new ArrayList<>();
  private List<FieldMetadata> _metricFieldSpecs = new ArrayList<>();
  private List<FieldMetadata> _dateTimeFieldSpecs = new ArrayList<>();
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

  public List<FieldMetadata> getDateTimeFieldSpecs() {
    return _dateTimeFieldSpecs;
  }

  public void setDateTimeFieldSpecs(List<FieldMetadata> dateTimeFieldSpecs) {
    _dateTimeFieldSpecs = dateTimeFieldSpecs;
  }

  public FieldMetadata getTimeFieldSpec() {
    return _timeFieldSpec;
  }

  public void setTimeFieldSpec(TimeFieldSpecMetadata timeFieldSpec) {
    _timeFieldSpec = timeFieldSpec;
  }
}
