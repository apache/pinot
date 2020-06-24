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
package org.apache.pinot.pql.parsers.pql2.ast;

import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.utils.request.FilterQueryTree;


/**
 * Common interface and implementation for predicate AST nodes.
 */
public abstract class PredicateAstNode extends BaseAstNode {
  protected String _identifier;

  /**
   * Create the query tree for the where clause
   *
   * @return
   */
  public abstract FilterQueryTree buildFilterQueryTree();

  /**
   * Create the query expression tree for the where clause
   *
   * @return
   */
  public abstract Expression buildFilterExpression();

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
