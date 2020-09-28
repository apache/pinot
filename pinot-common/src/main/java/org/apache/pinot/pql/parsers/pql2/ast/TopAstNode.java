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

import org.apache.pinot.common.exception.QueryException;


/**
 * AST node for the TOP clause.
 */
public class TopAstNode extends BaseAstNode {
  private int _count;
  public static int DEFAULT_TOP_N = 50;

  public TopAstNode(int count) {
    if (count < 0) {
      throw new RuntimeException(QueryException.QUERY_PARSING_ERROR);
    }
    if (count == 0) {
      _count = DEFAULT_TOP_N;
    } else {
      _count = count;
    }
  }

  public int getCount() {
    return _count;
  }

  @Override
  public String toString() {
    return "TopAstNode{" + "_count=" + _count + '}';
  }
}
