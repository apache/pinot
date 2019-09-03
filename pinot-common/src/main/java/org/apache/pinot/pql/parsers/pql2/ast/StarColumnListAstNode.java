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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.utils.request.RequestUtils;


/**
 * AST node for the star column list (as in SELECT * FROM foo).
 */
public class StarColumnListAstNode extends BaseAstNode {
  @Override
  public void updateBrokerRequest(BrokerRequest brokerRequest) {
    Selection selection = new Selection();
    List<String> modifiableList = new ArrayList<>(1);
    modifiableList.add("*");
    selection.setSelectionColumns(modifiableList);
    brokerRequest.setSelections(selection);
  }

  @Override
  public void updatePinotQuery(PinotQuery pinotQuery) {
    Expression starExpr = RequestUtils.createIdentifierExpression("*");
    pinotQuery.addToSelectList(starExpr);
  }
}
