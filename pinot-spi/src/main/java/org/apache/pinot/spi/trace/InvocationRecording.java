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
package org.apache.pinot.spi.trace;

import org.apache.pinot.spi.data.FieldSpec;


/**
 * Interface for recording data about query execution. By default, nothing happens with this data and regular tracing
 * is used. The user needs to register a {@see Tracer} implementation which produces recordings implementing this
 * interface for output to be captured.
 */
public interface InvocationRecording {

  /**
   * Sets the upper bound for the number of groups in a group by evaluation, along with the actual number of groups.
   * The ratio of these two quantities indicates how efficient the group by was, and if truncation has occurred.
   * @param numGroupsLimit the upper bound for the number of groups
   * @param numGroups the actual number of groups
   */
  default void setNumGroups(int numGroupsLimit, int numGroups) {
  }

  /**
   * Sets the number of docs scanned by the operator.
   * @param numDocsScanned how many docs were scanned.
   */
  default void setNumDocsScanned(int numDocsScanned) {
  }

  /**
   * Sets the number of documents matching after a filter has been applied.
   * Indicates whether the index was selective or not.
   * @param numDocsMatchingAfterFilter how many docs still match after applying the filter
   */
  default void setNumDocsMatchingAfterFilter(int numDocsMatchingAfterFilter) {
  }

  /**
   * Sets the number of bytes scanned by the operator if this is possible to compute.
   * @param bytesScanned the number of bytes scanned
   */
  default void setBytesProcessed(int bytesScanned) {
  }

  /**
   * @param numTasks the number of threads dispatched to
   */
  default void setNumTasks(int numTasks) {
  }

  /**
   * @param numChildren the number of children operators/transforms/projections
   */
  default void setNumChildren(int numChildren) {
  }

  /**
   * @param numSegments the number of segments
   */
  default void setNumSegments(int numSegments) {
  }

  /**
   * If the operator is a filter, determines the filter type (scan or index) and the predicate type
   * @param filterType SCAN or INDEX
   * @param predicateType e.g. BETWEEN, REGEXP_LIKE
   */
  default void setFilter(FilterType filterType, String predicateType) {
  }


  /**
   * Records the input datatype before a stage of query execution
   * @param dataType the output data type
   * @param singleValue if the output data type is single-value
   */
  default void setInputDataType(FieldSpec.DataType dataType, boolean singleValue) {
  }

  /**
   * Records the output datatype after a stage of query execution
   * @param dataType the output data type
   * @param singleValue if the output data type is single-value
   */
  default void setOutputDataType(FieldSpec.DataType dataType, boolean singleValue) {
  }

  /**
   * Records the range of docIds during the operator invocation. This is useful for implicating a range of records
   * in a slow operator invocation.
   * @param firstDocId the first docId in the block
   * @param lastDocId the last docId in the block
   */
  default void setDocIdRange(int firstDocId, int lastDocId) {
  }

  /**
   * If known, record the cardinality of the column within the segment this operator invoked on
   * @param cardinality the number of distinct values
   */
  default void setColumnCardinality(int cardinality) {
  }

  /**
   * @param columnName the name of the column operated on
   */
  default void setColumnName(String columnName) {
  }

  /**
   * @param functionName the function being evaluated
   */
  default void setFunctionName(String functionName) {
  }

  /**
   * @return true if the recording is enabled. If producing a value is costly, the caller should check whether
   * the recording is enabled before producing the value to keep tracing overhead down.
   */
  boolean isEnabled();
}
