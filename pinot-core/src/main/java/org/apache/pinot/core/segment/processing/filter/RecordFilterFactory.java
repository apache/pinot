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
package org.apache.pinot.core.segment.processing.filter;

import com.google.common.base.Preconditions;


/**
 * Factory for RecordFilter
 */
public final class RecordFilterFactory {

  private RecordFilterFactory() {

  }

  public enum RecordFilterType {
    NO_OP,
    /**
     * Evaluates a function expression to decide if the record should be filtered
     */
    FILTER_FUNCTION
  }

  /**
   * Construct a RecordFilter using the RecordFilterConfig
   */
  public static RecordFilter getRecordFilter(RecordFilterConfig config) {

    RecordFilter recordFilter = null;
    switch (config.getRecordFilterType()) {
      case NO_OP:
        recordFilter = new NoOpRecordFilter();
        break;
      case FILTER_FUNCTION:
        Preconditions.checkState(config.getFilterFunction() != null,
            "Must provide filterFunction for FILTER_FUNCTION record filter");
        recordFilter = new FunctionEvaluatorRecordFilter(config.getFilterFunction());
        break;
    }
    return recordFilter;
  }
}
