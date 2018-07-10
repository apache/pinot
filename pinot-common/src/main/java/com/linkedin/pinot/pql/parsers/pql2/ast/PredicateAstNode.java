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
package com.linkedin.pinot.pql.parsers.pql2.ast;

import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.HavingQueryTree;

/**
 * Common interface and implementation for predicate AST nodes.
 */
public abstract class PredicateAstNode extends BaseAstNode {
  protected String _identifier;
  protected FunctionCallAstNode _function;

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

  /**
   * Returns true if this predicate pertains to a function call (such as HAVING SUM(foo) = 5)
   *
   * @return true if this predicate pertains to a function call (such as HAVING SUM(foo) = 5)
   */
  public boolean isItFunctionCallComparison() {
    return _function != null;
  }

  /**
   * Returns the function call AST node for the function call on the left hand side of this predicate, or null if the
   * predicate is not based on a function call.
   *
   * @return The function call AST node for this predicate or null if there is no function call on the left hand side
   * of this predicate
   */
  public FunctionCallAstNode getFunction() {
    return _function;
  }

  /**
   * Returns the name of the identifier on the left hand side of this predicate, or null if the predicate is not based
   * on an identifier.
   *
   * @return The identifier or null if there is no identifier on the left hand side of this predicate
   */
  public String getIdentifier() {
    return _identifier;
  }

}
