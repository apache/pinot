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

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST node for a SELECT statement.
 */
public class SelectAstNode extends BaseAstNode {
  private String _tableName;
  private String _resourceName;
  private int _recordLimit = -1;
  private int _offset = -1;
  private int _topN = -1;

  // Optional clauses can be given in any order, so we keep track of whether we've already seen one
  private boolean _hasWhereClause = false;
  private boolean _hasGroupByClause = false;
  private boolean _hasHavingClause = false;
  private boolean _hasOrderByClause = false;
  private boolean _hasTopClause = false;
  private boolean _hasLimitClause = false;

  public SelectAstNode() {
  }

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof LimitAstNode) {
      if (_hasLimitClause) {
        throw new Pql2CompilationException("More than one LIMIT clause specified!");
      }

      LimitAstNode node = (LimitAstNode) childNode;
      _recordLimit = node.getCount();
      _offset = node.getOffset();
      _hasLimitClause = true;
    } else if (childNode instanceof TableNameAstNode) {
      TableNameAstNode node = (TableNameAstNode) childNode;
      _tableName = node.getTableName();
      _resourceName = node.getResourceName();
    } else if (childNode instanceof TopAstNode) {
      if (_hasTopClause) {
        throw new Pql2CompilationException("More than one TOP clause specified!");
      }

      TopAstNode node = (TopAstNode) childNode;
      _topN = node.getCount();
      _hasTopClause = true;
    } else if (childNode instanceof WhereAstNode) {
      if (_hasWhereClause) {
        throw new Pql2CompilationException("More than one WHERE clause specified!");
      }

      super.addChild(childNode);
      _hasWhereClause = true;
    } else if (childNode instanceof GroupByAstNode) {
      if (_hasGroupByClause) {
        throw new Pql2CompilationException("More than one GROUP BY clause specified!");
      }

      super.addChild(childNode);
      _hasGroupByClause = true;
    } else if (childNode instanceof HavingAstNode) {
      if (_hasHavingClause) {
        throw new Pql2CompilationException("More than one HAVING clause specified!");
      }

      super.addChild(childNode);
      _hasHavingClause = true;
    } else if (childNode instanceof OrderByAstNode) {
      if (_hasOrderByClause) {
        throw new Pql2CompilationException("More than one ORDER BY clause specified!");
      }

      super.addChild(childNode);
      _hasOrderByClause = true;
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
