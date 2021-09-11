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
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import org.apache.pinot.spi.data.FieldSpec;

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.DEFAULT_AVERAGE_NUM_VALUES_PER_ENTRY;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.DEFAULT_CARDINALITY;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.DEFAULT_DATA_LENGTH;


/**
 * The field information metadata piggybacked on a schema FieldSpec. For parsing the "schema with metadata" in the
 * input.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FieldMetadata extends FieldSpec {

  @JsonProperty("cardinality")
  int _cardinality = DEFAULT_CARDINALITY;

  @JsonProperty("averageLength")
  int _averageLength = DEFAULT_DATA_LENGTH;

  @JsonProperty("numValuesPerEntry")
  double _numValuesPerEntry = DEFAULT_AVERAGE_NUM_VALUES_PER_ENTRY; // for multi-values

  public int getAverageLength() {
    return _averageLength;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setAverageLength(int averageLength) {
    _averageLength = averageLength;
  }

  public double getNumValuesPerEntry() {
    return _numValuesPerEntry;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumValuesPerEntry(double numValuesPerEntry) {
    _numValuesPerEntry = numValuesPerEntry;
  }

  public int getCardinality() {
    return _cardinality;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setCardinality(int cardinality) {
    _cardinality = cardinality;
  }

  @Override
  public FieldType getFieldType() {
    return FieldType.DIMENSION;
  }
}
