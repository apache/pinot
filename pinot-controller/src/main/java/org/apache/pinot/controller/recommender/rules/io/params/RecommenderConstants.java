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

/**
 * The default parameters used int each algorithm and default values for general inputs
 * parameters usage are explained in the *Params class
 */
public class RecommenderConstants {
  private RecommenderConstants() {
  }

  public static class InvertedSortedIndexJointRule {
    public static final double DEFAULT_PERCENT_SELECT_FOR_FUNCTION = 0.5d;
    public static final double DEFAULT_PERCENT_SELECT_FOR_TEXT_MATCH = 0.5d;
    public static final double DEFAULT_PERCENT_SELECT_FOR_RANGE = 0.5d;
    public static final double DEFAULT_PERCENT_SELECT_FOR_REGEX = 0.5d;
    public static final double DEFAULT_PERCENT_SELECT_FOR_ISNULL = 0.5d;
    public static final double DEFAULT_THRESHOLD_MIN_AND_PREDICATE_INCREMENTAL_VOTE = 0.6d;
    public static final double DEFAULT_THRESHOLD_RATIO_MIN_AND_PREDICATE_TOP_CANDIDATES = 0.8d;
    public static final double DEFAULT_THRESHOLD_RATIO_MIN_GAIN_DIFF_BETWEEN_ITERATION = 0.05d;
    public static final int DEFAULT_MAX_NUM_ITERATION_WITHOUT_GAIN = 2;
    public static final double DEFAULT_THRESHOLD_RATIO_MIN_NESI_FOR_TOP_CANDIDATES = 0.7d;
  }

  public static class RulesToExecute {
    public static final boolean DEFAULT_RECOMMEND_SEGMENT_SIZE = true;
    public static final boolean DEFAULT_RECOMMEND_FLAG_QUERY = true;
    public static final boolean DEFAULT_RECOMMEND_VARIED_LENGTH_DICTIONARY = true;
    public static final boolean DEFAULT_RECOMMEND_KAFKA_PARTITION = true;
    public static final boolean DEFAULT_RECOMMEND_PINOT_TABLE_PARTITION = true;
    public static final boolean DEFAULT_RECOMMEND_INVERTED_SORTED_INDEX_JOINT = true;
    public static final boolean DEFAULT_RECOMMEND_BLOOM_FILTER = true;
    public static final boolean DEFAULT_RECOMMEND_RANGE_INDEX = true;
    public static final boolean DEFAULT_RECOMMEND_NO_DICTIONARY_ONHEAP_DICTIONARY_JOINT = true;
    public static final boolean DEFAULT_RECOMMEND_AGGREGATE_METRICS = true;
    public static final boolean DEFAULT_RECOMMEND_REALTIME_PROVISIONING = true;
  }

  public static class PartitionRule {
    public static final int DEFAULT_NUM_PARTITIONS = 1;

    public static final long DEFAULT_THRESHOLD_MAX_LATENCY_SLA_PARTITION = 1000;
    public static final long DEFAULT_THRESHOLD_MIN_QPS_PARTITION = 200;
    public static final long DEFAULT_KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION = 250;
    public static final double DEFAULT_THRESHOLD_RATIO_MIN_DIMENSION_PARTITION_TOP_CANDIDATES = 0.8d;
    public static final int DEFAULT_THRESHOLD_MAX_IN_LENGTH = 4;
    public static final double ACCEPTABLE_CARDINALITY_TO_NUM_PARTITIONS_RATIO = 0.7;
  }

  public static class BloomFilterRule {
    public static final long DEFAULT_THRESHOLD_MAX_CARDINALITY_BLOOMFILTER = 1000_000;
    public static final double DEFAULT_THRESHOLD_MIN_PERCENT_EQ_BLOOMFILTER = 0.5d;
  }

  public static class RangeIndexRule {
    public static final double DEFAULT_THRESHOLD_MIN_PERCENT_RANGE_INDEX = 0.4;
  }

  public static class NoDictionaryOnHeapDictionaryJointRule {
    public static final double DEFAULT_THRESHOLD_MIN_FILTER_FREQ_DICTIONARY = 0d;
    public static final double DEFAULT_THRESHOLD_MIN_SELECTION_FREQ_NO_DICTIONARY = 0.3d;
    public static final long DEFAULT_THRESHOLD_MIN_QPS_ON_HEAP = 10_000;
    public static final long DEFAULT_THRESHOLD_MAX_DICTIONARY_SIZE_ON_HEAP = 1000_000L;
    public static final double DEFAULT_THRESHOLD_MIN_FILTER_FREQ_ON_HEAP = 0.3d;
    public static final double DEFAULT_THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE = 0.95;
    public static final double DEFAULT_DICTIONARY_COEFFICIENT = 0.3;
  }

  public static class FlagQueryRuleParams {
    public static final long DEFAULT_THRESHOLD_MAX_LIMIT_SIZE = 100000;
    public static final String WARNING_NO_FILTERING =
        "Warning: Query seems to scan the entire table. No filters are used in the query. Please verify if filters "
            + "are not needed.";
    public static final String WARNING_NO_TIME_COL =
        "Warning: No time column used in filter in the query. Table with time columns typically use it in filters to "
            + "make the queries more selective.";
    public static final String WARNING_TOO_LONG_LIMIT =
        "Warning: Please verify if you need to pull out huge number of records for this query. Consider using smaller"
            + " limit than " + DEFAULT_THRESHOLD_MAX_LIMIT_SIZE;
    public static final String ERROR_INVALID_QUERY = "Error: Invalid query syntax. Please fix the query";
  }

  public static class RealtimeProvisioningRule {
    public static final int DEFAULT_REAL_TIME_TABLE_RETENTION_HOURS = 72;
    public static final String DEFAULT_MAX_USABLE_HOST_MEMORY = "48G";
    public static final int[] DEFAULT_NUM_HOURS = {2, 4, 6, 8, 10, 12};
    public static final int[] DEFAULT_NUM_HOSTS = {3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
  }

  public static class SegmentSizeRule {
    public static final int DEFAULT_NUM_SEGMENTS = 1;
    public static final int DEFAULT_DESIRED_SEGMENT_SIZE_MB = 500;
    public static final int NOT_PROVIDED = -1;
    public static final double INDEX_OVERHEAD_RATIO_FOR_SEGMENT_SIZE = 1.2;
  }

  public static final String PQL = "pql";
  public static final String SQL = "sql";
  public static final String OFFLINE = "offline";
  public static final String REALTIME = "realtime";
  public static final String HYBRID = "hybrid";
  public static final int DEFAULT_CARDINALITY = 1;
  public static final double MIN_CARDINALITY = 1;
  public static final double DEFAULT_AVERAGE_NUM_VALUES_PER_ENTRY = 1d;
  public static final int DEFAULT_NULL_SIZE = 0;
  public static final int DEFAULT_DATA_LENGTH = 100;
  public static final double EPSILON = 0.0001d; // used for double value comparison, margin of error
  public static final int DEFAULT_NUM_KAFKA_PARTITIONS = 0;
  public static final int DEFAULT_SEGMENT_FLUSH_TIME = 86400;
  public static final long DEFAULT_QPS = 5;
  public static final long DEFAULT_NUM_MESSAGES_PER_SEC_IN_KAFKA_TOPIC = 250;
  public static final long DEFAULT_NUM_RECORDS_PER_PUSH = 10000;
  public static final long DEFAULT_LATENCY_SLA = 500;

  public static final int NO_SUCH_COL = -1; // No such colname in colName to ID mapping

  public static final double THRESHOLD_MIN_USE_FPC = 0.05;
  public static final boolean DEFAULT_USE_CARDINALITY_NORMALIZATION = false;

  // In input queries, the IN predicate can follow the format of
  // a in ("#VALUES", 50) -> a in-predicate of length 50
  // a in (va1, val2, val3, val4) -> a in predicate of length 4
  public static final String IN_PREDICATE_ESTIMATE_LEN_FLAG = "#VALUES";
  public static final int FIRST = 0;
  public static final int SECOND = 1;

  public static final int DEFAULT_NUM_ROWS_IN_GENERATED_SEGMENT = 50_000;
}
