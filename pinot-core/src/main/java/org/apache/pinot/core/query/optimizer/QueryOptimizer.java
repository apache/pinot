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
package org.apache.pinot.core.query.optimizer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.optimizer.filter.FilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.FlattenAndOrFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.IdenticalPredicateFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.MergeEqInFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.MergeRangeFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.NumericalFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.TextMatchFilterOptimizer;
import org.apache.pinot.core.query.optimizer.filter.TimePredicateFilterOptimizer;
import org.apache.pinot.core.query.optimizer.statement.StatementOptimizer;
import org.apache.pinot.core.query.optimizer.statement.StringPredicateFilterOptimizer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class QueryOptimizer {
  // DO NOT change the order of these optimizers.
  // - MergeEqInFilterOptimizer, MergeRangeFilterOptimizer, and TextMatchFilterOptimizer each rely on
  //   FlattenAndOrFilterOptimizer to flatten the AND/OR predicate so that the children are on the same level to
  //   be merged
  // - TimePredicateFilterOptimizer and MergeRangeFilterOptimizer relies on NumericalFilterOptimizer to convert the
  //   values to the proper format so that they can be properly parsed
  private static final List<FilterOptimizer> FILTER_OPTIMIZERS =
      Arrays.asList(new FlattenAndOrFilterOptimizer(), new IdenticalPredicateFilterOptimizer(),
          new MergeEqInFilterOptimizer(), new NumericalFilterOptimizer(), new TimePredicateFilterOptimizer(),
          new MergeRangeFilterOptimizer(), new TextMatchFilterOptimizer());

  private static final List<StatementOptimizer> STATEMENT_OPTIMIZERS =
      Collections.singletonList(new StringPredicateFilterOptimizer());

  /**
   * Optimizes the given query.
   */
  public void optimize(PinotQuery pinotQuery, @Nullable Schema schema) {
    optimize(pinotQuery, null, schema);
  }

  /**
   * Optimizes the given query.
   */
  public void optimize(PinotQuery pinotQuery, @Nullable TableConfig tableConfig, @Nullable Schema schema) {
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      for (FilterOptimizer filterOptimizer : FILTER_OPTIMIZERS) {
        filterExpression = filterOptimizer.optimize(filterExpression, schema);
      }
      pinotQuery.setFilterExpression(filterExpression);
    }

    // Run statement optimizer after filter has already been optimized.
    for (StatementOptimizer statementOptimizer : STATEMENT_OPTIMIZERS) {
      statementOptimizer.optimize(pinotQuery, tableConfig, schema);
    }
  }
}
