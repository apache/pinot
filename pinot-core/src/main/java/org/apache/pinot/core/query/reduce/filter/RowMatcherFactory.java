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
package org.apache.pinot.core.query.reduce.filter;

import org.apache.pinot.common.request.context.FilterContext;


/**
 * Factory for RowMatcher.
 */
public class RowMatcherFactory {
  private RowMatcherFactory() {
  }

  /**
   * Helper method to construct a RowMatcher based on the given filter.
   */
  public static RowMatcher getRowMatcher(FilterContext filter, ValueExtractorFactory valueExtractorFactory,
      boolean nullHandlingEnabled) {
    switch (filter.getType()) {
      case AND:
        return new AndRowMatcher(filter.getChildren(), valueExtractorFactory, nullHandlingEnabled);
      case OR:
        return new OrRowMatcher(filter.getChildren(), valueExtractorFactory, nullHandlingEnabled);
      case NOT:
        assert filter.getChildren().size() == 1;
        return new NotRowMatcher(filter.getChildren().get(0), valueExtractorFactory, nullHandlingEnabled);
      case PREDICATE:
        return new PredicateRowMatcher(filter.getPredicate(),
            valueExtractorFactory.getValueExtractor(filter.getPredicate().getLhs()), nullHandlingEnabled);
      default:
        throw new IllegalStateException();
    }
  }
}
