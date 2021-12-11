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
  public Long _thresholdMinQpsOnHeap = DEFAULT_THRESHOLD_MIN_QPS_ON_HEAP;

  // We won't consider on heap dictionaries the frequency of this column used in filter < this threshold
  public Double _thresholdMinFilterFreqOnHeap = DEFAULT_THRESHOLD_MIN_FILTER_FREQ_ON_HEAP;

  // The maximum acceptable memory footprint on heap
  public Long _thresholdMaxDictionarySizeOnHeap = DEFAULT_THRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP;

  // For columns used in selection, if frequency >this threshold, we will apply no dictionary on it
  public Double _thresholdMinSelectionFreqNoDictionary = DEFAULT_THRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY;

  // For cols frequently used in filter or groupby, we will add dictionary on that, now default to 0,
  // meaning all cols ever used in filter or groupby will have dictionary
  public Double _thresholdMinFilterFreqDictionary = DEFAULT_THRESHOLD_MIN_FILTER_FREQ_DICTIONARY;

  // The accumulated size of dictionaries of all segments in one push is generally smaller than the whole big
  // dictionary size
  // (due to that the cardinality we have is the cardianlity for the whole dataset not per segment)
  // Use factor to shrink the size
  // TODO: improve this estimation if possible
  public Double _dictionaryCoefficient = DEFAULT_DICTIONARY_COEFFICIENT;

  // For colums not used in filter and selection, apply on heap dictionary only if it can save storage % > this
  // threshold
  public Double _thresholdMinPercentDictionaryStorageSave = DEFAULT_THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE;

  public Double getDictionaryCoefficient() {
    return _dictionaryCoefficient;
  }

  @JsonSetter(value = "DICTIONARY_COEFFICIENT", nulls = Nulls.SKIP)
  public void setDictionaryCoefficient(Double dictionaryCoefficient) {
    _dictionaryCoefficient = dictionaryCoefficient;
  }

  public Double getThresholdMinPercentDictionaryStorageSave() {
    return _thresholdMinPercentDictionaryStorageSave;
  }

  @JsonSetter(value = "THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE", nulls = Nulls.SKIP)
  public void setThresholdMinPercentDictionaryStorageSave(Double thresholdMinPercentDictionaryStorageSave) {
    _thresholdMinPercentDictionaryStorageSave = thresholdMinPercentDictionaryStorageSave;
  }

  public Double getThresholdMinFilterFreqOnHeap() {
    return _thresholdMinFilterFreqOnHeap;
  }

  @JsonSetter(value = "THRESHOLD_MIN_FILTER_FREQ_ON_HEAP", nulls = Nulls.SKIP)
  public void setThresholdMinFilterFreqOnHeap(Double thresholdMinFilterFreqOnHeap) {
    _thresholdMinFilterFreqOnHeap = thresholdMinFilterFreqOnHeap;
  }

  public Long getThresholdMaxDictionarySizeOnHeap() {
    return _thresholdMaxDictionarySizeOnHeap;
  }

  @JsonSetter(value = "THRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP", nulls = Nulls.SKIP)
  public void setThresholdMaxDictionarySizeOnHeap(Long thresholdMaxDictionarySizeOnHeap) {
    _thresholdMaxDictionarySizeOnHeap = thresholdMaxDictionarySizeOnHeap;
  }

  public Long getThresholdMinQpsOnHeap() {
    return _thresholdMinQpsOnHeap;
  }

  @JsonSetter(value = "THRESHOLD_MIN_QPS_ON_HEAP", nulls = Nulls.SKIP)
  public void setThresholdMinQpsOnHeap(Long thresholdMinQpsOnHeap) {
    _thresholdMinQpsOnHeap = thresholdMinQpsOnHeap;
  }

  public Double getThresholdMinSelectionFreqNoDictionary() {
    return _thresholdMinSelectionFreqNoDictionary;
  }

  @JsonSetter(value = "THRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY", nulls = Nulls.SKIP)
  public void setThresholdMinSelectionFreqNoDictionary(Double thresholdMinSelectionFreqNoDictionary) {
    _thresholdMinSelectionFreqNoDictionary = thresholdMinSelectionFreqNoDictionary;
  }

  public Double getThresholdMinFilterFreqDictionary() {
    return _thresholdMinFilterFreqDictionary;
  }

  @JsonSetter(value = "THRESHOLD_MIN_FILTER_FREQ_DICTIONARY", nulls = Nulls.SKIP)
  public void setThresholdMinFilterFreqDictionary(Double thresholdMinFilterFreqDictionary) {
    _thresholdMinFilterFreqDictionary = thresholdMinFilterFreqDictionary;
  }
}
