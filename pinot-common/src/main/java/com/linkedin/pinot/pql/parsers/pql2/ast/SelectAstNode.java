/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.request.Selection;


/**
 * AST node for a SELECT statement.
 */
public class SelectAstNode extends BaseAstNode {
  private String _tableName;
  private String _resourceName;
  private int _recordLimit = -1;
  private int _offset = -1;
  private int _topN = -1;

  public SelectAstNode() {
  }

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof LimitAstNode) {
      LimitAstNode node = (LimitAstNode) childNode;
      _recordLimit = node.getCount();
      _offset = node.getOffset();
    } else if (childNode instanceof TableNameAstNode) {
      TableNameAstNode node = (TableNameAstNode) childNode;
      _tableName = node.getTableName();
      _resourceName = node.getResourceName();
    } else if (childNode instanceof TopAstNode) {
      TopAstNode node = (TopAstNode) childNode;
      _topN = node.getCount();
    } else {
      super.addChild(childNode);
    }
  }

  @Override
  public String toString() {
    return "SelectAstNode{" +
        "_tableName='" + _tableName + '\'' +
        ", _resourceName='" + _resourceName + '\'' +
        ", _recordLimit=" + _recordLimit +
        ", _offset=" + _offset +
        '}';
  }

  @Override
  public void updateBrokerRequest(BrokerRequest brokerRequest) {
    // Set the query source
    QuerySource querySource = new QuerySource();
    querySource.setTableName(_resourceName);
    brokerRequest.setQuerySource(querySource);

    sendBrokerRequestUpdateToChildren(brokerRequest);

    // If there is a selection, set its limit if applicable
    Selection selections = brokerRequest.getSelections();
    if (selections != null) {
      if (_recordLimit != -1) {
        selections.setSize(_recordLimit);
      }
      if (_offset != -1) {
        selections.setOffset(_offset);
      }
    }

    // If there is a topN clause, set it on the group by
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (groupBy != null) {
      if (_topN != -1) {
        groupBy.setTopN(_topN);
      } else {
        // Pinot quirk: default to top 10
        groupBy.setTopN(10);
      }
    }

    // Pinot quirk: if there is both a selection and an aggregation, remove the selection
    if (brokerRequest.getAggregationsInfoSize() != 0 && brokerRequest.isSetSelections()) {
      brokerRequest.setSelections(null);
    }
  }
}
