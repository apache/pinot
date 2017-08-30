/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.pql.parsers.pql2.ast;

import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.HavingQueryTree;
import com.linkedin.pinot.common.utils.request.QueryTree;

/**
 * Common interface for predicate AST nodes.
 */
public abstract class PredicateAstNode extends BaseAstNode {

  /**
   * Create the query tree for the where clause
   *
   * @return
   *
   */
  public abstract FilterQueryTree buildFilterQueryTree();

  /**
   * Create the query tree for the having clause
   * It is different than FilterQuery because here we deal with function call comparison with literals
   * @return
   */
  public abstract HavingQueryTree buildHavingQueryTree();

}
