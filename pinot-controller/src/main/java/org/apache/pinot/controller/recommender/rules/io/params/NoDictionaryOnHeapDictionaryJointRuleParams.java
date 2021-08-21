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
package org.apache.pinot.controller.recommender.rules.io.params;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.NoDictionaryOnHeapDictionaryJointRule.*;


/**
 * Thresholds and parameters used in NoDictionaryOnHeapDictionaryJointRule
 */
public class NoDictionaryOnHeapDictionaryJointRuleParams {

  // We won't consider on heap dictionaries if table QPS < this threshold
  public Long THRESHOLD_MIN_QPS_ON_HEAP = DEFAULT_THRESHOLD_MIN_QPS_ON_HEAP;

  // We won't consider on heap dictionaries the frequency of this column used in filter < this threshold
  public Double THRESHOLD_MIN_FILTER_FREQ_ON_HEAP = DEFAULT_THRESHOLD_MIN_FILTER_FREQ_ON_HEAP;

  // The maximum acceptable memory footprint on heap
  public Long THRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP = DEFAULT_THRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP;

  // For columns used in selection, if frequency >this threshold, we will apply no dictionary on it
  public Double THRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY = DEFAULT_THRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY;

  // For cols frequently used in filter or groupby, we will add dictionary on that, now default to 0,
  // meaning all cols ever used in filter or groupby will have dictionary
  public Double THRESHOLD_MIN_FILTER_FREQ_DICTIONARY = DEFAULT_THRESHOLD_MIN_FILTER_FREQ_DICTIONARY;

  // The accumulated size of dictionaries of all segments in one push is generally smaller than the whole big
  // dictionary size
  // (due to that the cardinality we have is the cardianlity for the whole dataset not per segment)
  // Use factor to shrink the size
  // TODO: improve this estimation if possible
  public Double DICTIONARY_COEFFICIENT = DEFAULT_DICTIONARY_COEFFICIENT;

  // For colums not used in filter and selection, apply on heap dictionary only if it can save storage % > this
  // threshold
  public Double THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE = DEFAULT_THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE;

  public Double getDICTIONARY_COEFFICIENT() {
    return DICTIONARY_COEFFICIENT;
  }

  @JsonSetter(value = "DICTIONARY_COEFFICIENT", nulls = Nulls.SKIP)
  public void setDICTIONARY_COEFFICIENT(Double DICTIONARY_COEFFICIENT) {
    this.DICTIONARY_COEFFICIENT = DICTIONARY_COEFFICIENT;
  }

  public Double getTHRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE() {
    return THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE;
  }

  @JsonSetter(value = "THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE(Double THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE) {
    this.THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE = THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE;
  }

  public Double getTHRESHOLD_MIN_FILTER_FREQ_ON_HEAP() {
    return THRESHOLD_MIN_FILTER_FREQ_ON_HEAP;
  }

  @JsonSetter(value = "THRESHOLD_MIN_FILTER_FREQ_ON_HEAP", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MIN_FILTER_FREQ_ON_HEAP(Double THRESHOLD_MIN_FILTER_FREQ_ON_HEAP) {
    this.THRESHOLD_MIN_FILTER_FREQ_ON_HEAP = THRESHOLD_MIN_FILTER_FREQ_ON_HEAP;
  }

  public Long getTHRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP() {
    return THRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP;
  }

  @JsonSetter(value = "THRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP(Long THRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP) {
    this.THRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP = THRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP;
  }

  public Long getTHRESHOLD_MIN_QPS_ON_HEAP() {
    return THRESHOLD_MIN_QPS_ON_HEAP;
  }

  @JsonSetter(value = "THRESHOLD_MIN_QPS_ON_HEAP", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MIN_QPS_ON_HEAP(Long THRESHOLD_MIN_QPS_ON_HEAP) {
    this.THRESHOLD_MIN_QPS_ON_HEAP = THRESHOLD_MIN_QPS_ON_HEAP;
  }

  public Double getTHRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY() {
    return THRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY;
  }

  @JsonSetter(value = "THRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY(Double THRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY) {
    this.THRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY = THRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY;
  }

  public Double getTHRESHOLD_MIN_FILTER_FREQ_DICTIONARY() {
    return THRESHOLD_MIN_FILTER_FREQ_DICTIONARY;
  }

  @JsonSetter(value = "THRESHOLD_MIN_FILTER_FREQ_DICTIONARY", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MIN_FILTER_FREQ_DICTIONARY(Double THRESHOLD_MIN_FILTER_FREQ_DICTIONARY) {
    this.THRESHOLD_MIN_FILTER_FREQ_DICTIONARY = THRESHOLD_MIN_FILTER_FREQ_DICTIONARY;
  }
}