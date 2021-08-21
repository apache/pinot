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

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST node for a SELECT statement.
 */
public class SelectAstNode extends BaseAstNode {
  public static final int DEFAULT_RECORD_LIMIT = 10;
  private String _tableName;
  private String _resourceName;
  private int _recordLimit = -1;
  private int _offset = -1;
  private int _topN = -1;
  // Optional clauses can be given in any order, so we keep track of whether we've already seen one
  private boolean _hasWhereClause = false;
  private boolean _hasGroupByClause = false;
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
    return "SelectAstNode{" + "_tableName='" + _tableName + '\'' + ", _resourceName='" + _resourceName + '\''
        + ", _recordLimit=" + _recordLimit + ", _offset=" + _offset + '}';
  }

  @Override
  public void updateBrokerRequest(BrokerRequest brokerRequest) {
    // Set the query source
    QuerySource querySource = new QuerySource();
    querySource.setTableName(_tableName);
    brokerRequest.setQuerySource(querySource);

    sendBrokerRequestUpdateToChildren(brokerRequest);

    // If there is a selection, set its limit if applicable
    Selection selections = brokerRequest.getSelections();
    if (selections != null) {
      if (_recordLimit != -1) {
        selections.setSize(_recordLimit);
      } else {
        // Pinot quirk: default to LIMIT 10
        selections.setSize(DEFAULT_RECORD_LIMIT);
      }
      if (_offset != -1) {
        selections.setOffset(_offset);
      }
    }

    // If there is a topN clause, set it on the group by
    // if topN is not present, set it with LIMIT;
    // if Limit is not present, set it with DEFAULT.
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (groupBy != null) {
      if (_topN != -1) {
        groupBy.setTopN(_topN);
      } else if (_recordLimit > 0) {
        groupBy.setTopN(_recordLimit);
      } else {
        // Pinot quirk: default to top 10
        groupBy.setTopN(DEFAULT_RECORD_LIMIT);
      }
    }

    // set the limit in broker request so that it can be used by
    // other queries too (that don't go down the selection path in execution engine)
    // e.g SELECT DISTINCT(col1, col2) FROM foo LIMIT 1000
    //
    // the current implementation restricted the use of LIMIT clause only to standard
    // select queries like SELECT C1, C2 FROM foo LIMIT 100 and thus the LIMIT info
    // was stored in BrokerRequest.getSelections().setSize().
    //
    // However, now LIMIT is also applicable to DISTINCT. But since DISTINCT follows
    // the aggregation execution code path, we need to store the LIMIT info in a common
    // place that can be leveraged by all the queries regardless of whether they are
    // a selection query or aggregation query. Thus the limit is stored in broker
    // to easily access it anywhere in the physical planning or execution code.
    if (_recordLimit != -1) {
      brokerRequest.setLimit(_recordLimit);
    } else {
      brokerRequest.setLimit(DEFAULT_RECORD_LIMIT);
    }

    // Pinot quirk: if there is both a selection and an aggregation, remove the selection
    if (brokerRequest.getAggregationsInfoSize() != 0 && brokerRequest.isSetSelections()) {
      brokerRequest.setSelections(null);
    }
  }

  @Override
  public void updatePinotQuery(PinotQuery pinotQuery) {
    // Set data source
    DataSource dataSource = new DataSource();
    dataSource.setTableName(_tableName);
    pinotQuery.setDataSource(dataSource);
    sendPinotQueryUpdateToChildren(pinotQuery);
    if (_offset != -1) {
      pinotQuery.setOffset(_offset);
    }
    if (pinotQuery.getGroupByListSize() > 0) {
      // Handle GroupBy
      if (_topN != -1) {
        pinotQuery.setLimit(_topN);
      } else if (_recordLimit > 0) {
        pinotQuery.setLimit(_recordLimit);
      } else {
        // Pinot quirk: default to top 10
        pinotQuery.setLimit(10);
      }
    } else {
      // Handle Selection
      if (_recordLimit != -1) {
        pinotQuery.setLimit(_recordLimit);
      } else {
        pinotQuery.setLimit(10);
      }
    }
  }

  public boolean isHasTopClause() {
    return _hasTopClause;
  }

  public int getTopN() {
    return _topN;
  }

  public boolean isHasLimitClause() {
    return _hasLimitClause;
  }

  public int getRecordLimit() {
    return _recordLimit;
  }
}
