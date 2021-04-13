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
package org.apache.pinot.controller.recommender.rules;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.impl.AggregateMetricsRule;
import org.apache.pinot.controller.recommender.rules.impl.BloomFilterRule;
import org.apache.pinot.controller.recommender.rules.impl.FlagQueryRule;
import org.apache.pinot.controller.recommender.rules.impl.InvertedSortedIndexJointRule;
import org.apache.pinot.controller.recommender.rules.impl.KafkaPartitionRule;
import org.apache.pinot.controller.recommender.rules.impl.NoDictionaryOnHeapDictionaryJointRule;
import org.apache.pinot.controller.recommender.rules.impl.PinotTablePartitionRule;
import org.apache.pinot.controller.recommender.rules.impl.RealtimeProvisioningRule;
import org.apache.pinot.controller.recommender.rules.impl.VariedLengthDictionaryRule;

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.RulesToExecute.*;


/**
 * In this class we will have
 * RuleFactory: factory for all rules, please add constructor for new rules in this factory wen extending
 * booleans to dictate whether a rule needs to be fired
 * Rule: a enum with all the rule names
 */
public class RulesToExecute {
  public static class RuleFactory {
    public static AbstractRule getRule(Rule rule, InputManager inputManager, ConfigManager outputManager) {
      switch (rule) {
        case FlagQueryRule:
          return new FlagQueryRule(inputManager, outputManager);
        case InvertedSortedIndexJointRule:
          return new InvertedSortedIndexJointRule(inputManager, outputManager);
        case KafkaPartitionRule:
          return new KafkaPartitionRule(inputManager, outputManager);
        case PinotTablePartitionRule:
          return new PinotTablePartitionRule(inputManager, outputManager);
        case BloomFilterRule:
          return new BloomFilterRule(inputManager, outputManager);
        case NoDictionaryOnHeapDictionaryJointRule:
          return new NoDictionaryOnHeapDictionaryJointRule(inputManager, outputManager);
        case VariedLengthDictionaryRule:
          return new VariedLengthDictionaryRule(inputManager, outputManager);
        case AggregateMetricsRule:
          return new AggregateMetricsRule(inputManager, outputManager);
        case RealtimeProvisioningRule:
          return new RealtimeProvisioningRule(inputManager, outputManager);
        default:
          return null;
      }
    }
  }
  // All rules will execute by default unless explicitly specifying "recommendInvertedSortedIndexJoint" = "false"
  boolean _recommendKafkaPartition = DEFAULT_RECOMMEND_KAFKA_PARTITION;
  boolean _recommendPinotTablePartition = DEFAULT_RECOMMEND_PINOT_TABLE_PARTITION;
  boolean _recommendInvertedSortedIndexJoint = DEFAULT_RECOMMEND_INVERTED_SORTED_INDEX_JOINT;
  boolean _recommendBloomFilter = DEFAULT_RECOMMEND_BLOOM_FILTER;
  boolean _recommendNoDictionaryOnHeapDictionaryJoint = DEFAULT_RECOMMEND_NO_DICTIONARY_ONHEAP_DICTIONARY_JOINT;
  boolean _recommendVariedLengthDictionary = DEFAULT_RECOMMEND_VARIED_LENGTH_DICTIONARY;
  boolean _recommendFlagQuery = DEFAULT_RECOMMEND_FLAG_QUERY;
  boolean _recommendAggregateMetrics = DEFAULT_RECOMMEND_AGGREGATE_METRICS;
  boolean _recommendRealtimeProvisioning = DEFAULT_RECOMMEND_REALTIME_PROVISIONING;

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRecommendVariedLengthDictionary(boolean recommendVariedLengthDictionary) {
    _recommendVariedLengthDictionary = recommendVariedLengthDictionary;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRecommendFlagQuery(boolean recommendFlagQuery) {
    _recommendFlagQuery = recommendFlagQuery;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRecommendNoDictionaryOnHeapDictionaryJoint(boolean recommendNoDictionaryOnHeapDictionaryJoint) {
    _recommendNoDictionaryOnHeapDictionaryJoint = recommendNoDictionaryOnHeapDictionaryJoint;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRecommendKafkaPartition(boolean recommendKafkaPartition) {
    _recommendKafkaPartition = recommendKafkaPartition;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRecommendInvertedSortedIndexJoint(boolean recommendInvertedSortedIndexJoint) {
    _recommendInvertedSortedIndexJoint = recommendInvertedSortedIndexJoint;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRecommendPinotTablePartition(boolean recommendPinotTablePartition) {
    _recommendPinotTablePartition = recommendPinotTablePartition;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRecommendBloomFilter(boolean recommendBloomFilter) {
    _recommendBloomFilter = recommendBloomFilter;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRecommendAggregateMetrics(boolean aggregateMetrics) {
    _recommendAggregateMetrics = aggregateMetrics;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRecommendRealtimeProvisioning(boolean recommendRealtimeProvisioning) {
    _recommendPinotTablePartition = recommendRealtimeProvisioning;
  }

  public boolean isRecommendVariedLengthDictionary() {
    return _recommendVariedLengthDictionary;
  }

  public boolean isRecommendFlagQuery() {
    return _recommendFlagQuery;
  }

  public boolean isRecommendNoDictionaryOnHeapDictionaryJoint() {
    return _recommendNoDictionaryOnHeapDictionaryJoint;
  }

  public boolean isRecommendKafkaPartition() {
    return _recommendKafkaPartition;
  }

  public boolean isRecommendInvertedSortedIndexJoint() {
    return _recommendInvertedSortedIndexJoint;
  }

  public boolean isRecommendPinotTablePartition() {
    return _recommendPinotTablePartition;
  }

  public boolean isRecommendBloomFilter() {
    return _recommendBloomFilter;
  }

  public boolean isRecommendAggregateMetrics() {
    return _recommendAggregateMetrics;
  }

  public boolean isRecommendRealtimeProvisioning() {
    return _recommendRealtimeProvisioning;
  }

  // Be careful with the sequence, each rule can execute individually
  // but a rule may depend on its previous rule when they both fired
  public enum Rule {
    FlagQueryRule,
    KafkaPartitionRule,
    InvertedSortedIndexJointRule,
    NoDictionaryOnHeapDictionaryJointRule, // NoDictionaryOnHeapDictionaryJointRule must go after InvertedSortedIndexJointRule since we do not recommend NoDictionary on cols with indices
    VariedLengthDictionaryRule, // VariedLengthDictionaryRule must go after NoDictionaryOnHeapDictionaryJointRule  since we do not recommend dictionary on NoDictionary cols
    PinotTablePartitionRule, // PinotTablePartitionRule must go after KafkaPartitionRule to recommend realtime partitions, after NoDictionaryOnHeapDictionaryJointRule to correctly calculate record size
    BloomFilterRule,
    AggregateMetricsRule,
    RealtimeProvisioningRule // this rule must be the last one because it needs the output of other rules as its input
  }
}
