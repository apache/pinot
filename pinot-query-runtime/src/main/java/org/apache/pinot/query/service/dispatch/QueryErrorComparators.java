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
package org.apache.pinot.query.service.dispatch;

import java.util.Map;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * Shared comparator utilities for ranking {@link QueryErrorCode} entries produced during query execution.
 *
 * <p>{@link QueryErrorCode#QUERY_VALIDATION} errors are always ranked highest (the "max" of the comparator ordering).
 * Within any other error codes the comparison falls back to {@link QueryErrorCode#getId()} ordering.
 *
 * <p>This class is package-private; it exists solely to de-duplicate the identical comparator that was previously
 * copy-pasted in {@link QueryDispatcher} and {@link LazyBrokerResponse}.
 */
// TODO: Improve the way the errors are compared
final class QueryErrorComparators {
  private QueryErrorComparators() {
  }

  /**
   * Compares two {@link Map.Entry} instances by their {@link QueryErrorCode} key, ranking
   * {@link QueryErrorCode#QUERY_VALIDATION} errors as the highest priority.
   *
   * <p>The method is suitable for use as a {@link java.util.Comparator} (e.g. with
   * {@link java.util.stream.Stream#max(java.util.Comparator)}) to select the most important error from a collection.
   *
   * <p><b>Comparator contract note:</b> when both entries carry {@code QUERY_VALIDATION} the method returns {@code 0}
   * (equal).  Without this branch, {@code compare(a, b)} and {@code compare(b, a)} would both return {@code 1},
   * violating antisymmetry and risking {@link IllegalArgumentException} under TimSort.
   *
   * @param entry1 the first entry
   * @param entry2 the second entry
   * @return a negative integer, zero, or a positive integer as {@code entry1} is less than, equal to, or greater than
   * {@code entry2}
   */
  static int compareErrors(Map.Entry<QueryErrorCode, String> entry1, Map.Entry<QueryErrorCode, String> entry2) {
    QueryErrorCode errorCode1 = entry1.getKey();
    QueryErrorCode errorCode2 = entry2.getKey();
    if (errorCode1 == QueryErrorCode.QUERY_VALIDATION && errorCode2 == QueryErrorCode.QUERY_VALIDATION) {
      // both are QUERY_VALIDATION — treat as equal; preserves comparator antisymmetry
      return 0;
    }
    if (errorCode1 == QueryErrorCode.QUERY_VALIDATION) {
      return 1;
    }
    if (errorCode2 == QueryErrorCode.QUERY_VALIDATION) {
      return -1;
    }
    return Integer.compare(errorCode1.getId(), errorCode2.getId());
  }
}
