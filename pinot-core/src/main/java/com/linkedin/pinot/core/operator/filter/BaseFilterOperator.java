/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.filter;

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import javax.annotation.Nullable;

/**
 * Base Operator for all filter operators. ResultBlock is initialized in the planning phase
 *
 */
public abstract class BaseFilterOperator extends BaseOperator<BaseFilterBlock> {

  protected DataSource _dataSource;

  protected PredicateEvaluator _predicateEvaluator;

  /**
   * Return whether the result is empty.
   */
  public abstract boolean isResultEmpty();

  /**
   * Returns the datasource metadata
   */
  public @Nullable DataSourceMetadata getDataSourceMetadata() {
    if (_dataSource == null) return null;

    return _dataSource.getDataSourceMetadata();
  }

  /**
   * Return the predicate evaluator associated with the filter.
   */
  public @Nullable PredicateEvaluator getPredicateEvaluator() {
    return _predicateEvaluator;
  }
}
