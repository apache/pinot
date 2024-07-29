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

package org.apache.pinot.controller.recommender.rules.impl;

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.params.FlagQueryRuleParams;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.FlagQueryRuleParams.WARNING_NO_FILTERING;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.FlagQueryRuleParams.WARNING_NO_TIME_COL;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.FlagQueryRuleParams.WARNING_TOO_LONG_LIMIT;


/**
 * Flag the queries that are not valid:
 *    Flag the queries with LIMIT value higher than a threshold.
 *    Flag the queries that are not using any filters.
 *    Flag the queries that are not using any time filters.
 */
public class FlagQueryRule extends AbstractRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlagQueryRule.class);
  private final FlagQueryRuleParams _params;

  public FlagQueryRule(InputManager input, ConfigManager output) {
    super(input, output);
    _params = input.getFlagQueryRuleParams();
  }

  @Override
  public void run() {
    for (String query : _input.getParsedQueries()) {
      LOGGER.debug("Parsing query: {}", query);
      QueryContext queryContext = _input.getQueryContext(query);
      if (queryContext.getLimit() > _params._thresholdMaxLimitSize) {
        //Flag the queries with LIMIT value higher than a threshold.
        _output.getFlaggedQueries().add(query, WARNING_TOO_LONG_LIMIT);
      }

      FilterContext filter = queryContext.getFilter();
      if (filter == null || filter.isConstant()) {
        //Flag the queries that are not using any filters.
        _output.getFlaggedQueries().add(query, WARNING_NO_FILTERING);
      } else { //Flag the queries that are not using any time filters.
        Set<String> usedCols = new HashSet<>();
        filter.getColumns(usedCols);
        Set<String> timeCols = _input.getTimeColumns();
        if (!timeCols.isEmpty()) {
          usedCols.retainAll(timeCols);
          if (usedCols.isEmpty()) {
            _output.getFlaggedQueries().add(query, WARNING_NO_TIME_COL);
          }
        }
      }
    }
  }
}
