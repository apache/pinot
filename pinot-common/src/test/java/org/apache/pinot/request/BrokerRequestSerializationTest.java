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
package org.apache.pinot.request;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.request.QueryType;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.pql2.ast.OrderByAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.StringLiteralAstNode;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.testng.annotations.Test;


public class BrokerRequestSerializationTest {

  @Test
  public static void testSerialization()
      throws TException {
    BrokerRequest req = new BrokerRequest();

    // Populate Query Type
    QueryType type = new QueryType();
    type.setHasAggregation(true);
    type.setHasFilter(true);
    type.setHasSelection(true);
    type.setHasGroup_by(true);
    req.setQueryType(type);

    // Populate Query source
    QuerySource s = new QuerySource();
    s.setTableName("dummy");
    req.setQuerySource(s);

    req.setDuration("dummy");
    req.setTimeInterval("dummy");

    //Populate Group-By
    GroupBy groupBy = new GroupBy();
    List<String> columns = new ArrayList<String>();
    columns.add("dummy1");
    columns.add("dummy2");
    groupBy.setColumns(columns);
    groupBy.setTopN(100);
    req.setGroupBy(groupBy);

    //Populate Selections
    Selection sel = new Selection();
    sel.setSize(1);
    SelectionSort s2 = new SelectionSort();
    s2.setColumn("dummy1");
    s2.setIsAsc(true);
    sel.addToSelectionSortSequence(s2);
    sel.addToSelectionColumns("dummy1");
    req.setSelections(sel);

    //Populate FilterQuery
    FilterQuery q1 = new FilterQuery();
    q1.setId(1);
    q1.setColumn("dummy1");
    q1.addToValue("dummy1");
    q1.addToNestedFilterQueryIds(2);
    q1.setOperator(FilterOperator.AND);
    FilterQuery q2 = new FilterQuery();
    q2.setId(2);
    q2.setColumn("dummy2");
    q2.addToValue("dummy2");
    q2.setOperator(FilterOperator.AND);

    FilterQueryMap map = new FilterQueryMap();
    map.putToFilterQueryMap(1, q1);
    map.putToFilterQueryMap(2, q2);
    req.setFilterQuery(q1);
    req.setFilterSubQueryMap(map);

    //Populate Aggregations
    AggregationInfo agg = new AggregationInfo();
    agg.setAggregationType("dummy1");
    agg.putToAggregationParams("key1", "dummy1");
    req.addToAggregationsInfo(agg);

    TSerializer normalSerializer = new TSerializer();
    TSerializer compactSerializer = new TSerializer(new TCompactProtocol.Factory());
    normalSerializer.serialize(req);
    compactSerializer.serialize(req);

//    int numRequests = 100000;
//    TimerContext t = MetricsHelper.startTimer();
//    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
//    //TSerializer serializer = new TSerializer();
//    //Compact : Size 183 , Serialization Latency : 0.03361ms
//    // Normal : Size 385 , Serialization Latency : 0.01144ms
//
//    for (int i = 0; i < numRequests; i++) {
//      try {
//        serializer.serialize(req);
//        //System.out.println(s3.length);
//        //break;
//      } catch (TException e) {
//        e.printStackTrace();
//      }
//    }
//    t.stop();
//    System.out.println("Latency is :" + (t.getLatencyMs() / (float) numRequests));
  }

  @Test
  public static void testSerializationWithPinotQuery()
      throws TException {
    BrokerRequest req = new BrokerRequest();

    // START Set PinotQuery
    PinotQuery pinotQuery = new PinotQuery();

    // Populate Query source
    DataSource dataSource = new DataSource();
    dataSource.setTableName("dummy");
    pinotQuery.setDataSource(dataSource);

    //Populate Group-By

    List<Expression> groupByList = new ArrayList<>();
    groupByList.add(RequestUtils.createIdentifierExpression("dummy1"));
    groupByList.add(RequestUtils.createIdentifierExpression("dummy2"));
    pinotQuery.setGroupByList(groupByList);
    pinotQuery.setLimit(100);

    //Populate Selections
    pinotQuery.addToSelectList(RequestUtils.createIdentifierExpression("dummy1"));

    //Populate OrderBy
    Expression ascExpr = RequestUtils.getFunctionExpression(OrderByAstNode.ASCENDING_ORDER);
    ascExpr.getFunctionCall().addToOperands(RequestUtils.createIdentifierExpression("dummy1"));
    pinotQuery.addToOrderByList(ascExpr);

    //Populate FilterQuery
    Expression filter = RequestUtils.getFunctionExpression(FilterOperator.AND.name());

    Expression filterExpr1 = RequestUtils.getFunctionExpression(FilterOperator.EQUALITY.name());
    filterExpr1.getFunctionCall().addToOperands(RequestUtils.createIdentifierExpression("dummy1"));
    filterExpr1.getFunctionCall()
        .addToOperands(RequestUtils.createLiteralExpression(new StringLiteralAstNode("dummy1")));
    filter.getFunctionCall().addToOperands(filterExpr1);

    Expression filterExpr2 = RequestUtils.getFunctionExpression(FilterOperator.EQUALITY.name());
    filterExpr2.getFunctionCall().addToOperands(RequestUtils.createIdentifierExpression("dummy2"));
    filterExpr2.getFunctionCall()
        .addToOperands(RequestUtils.createLiteralExpression(new StringLiteralAstNode("dummy2")));
    filter.getFunctionCall().addToOperands(filterExpr2);

    pinotQuery.setFilterExpression(filter);

    //Populate Aggregations
    Expression aggExpr = RequestUtils.getFunctionExpression("dummy1");
    aggExpr.getFunctionCall().addToOperands(RequestUtils.createIdentifierExpression("dummy"));
    pinotQuery.addToSelectList(aggExpr);

    req.setPinotQuery(pinotQuery);

    // END Set PinotQuery

    // Populate Query Type
    QueryType type = new QueryType();
    type.setHasAggregation(true);
    type.setHasFilter(true);
    type.setHasSelection(true);
    type.setHasGroup_by(true);
    req.setQueryType(type);

    // Populate Query source
    QuerySource s = new QuerySource();
    s.setTableName("dummy");
    req.setQuerySource(s);

    req.setDuration("dummy");
    req.setTimeInterval("dummy");

    //Populate Group-By
    GroupBy groupBy = new GroupBy();
    List<String> columns = new ArrayList<String>();
    columns.add("dummy1");
    columns.add("dummy2");
    groupBy.setColumns(columns);
    groupBy.setTopN(100);
    req.setGroupBy(groupBy);

    //Populate Selections
    Selection sel = new Selection();
    sel.setSize(1);
    SelectionSort s2 = new SelectionSort();
    s2.setColumn("dummy1");
    s2.setIsAsc(true);
    sel.addToSelectionSortSequence(s2);
    sel.addToSelectionColumns("dummy1");
    req.setSelections(sel);

    //Populate FilterQuery
    FilterQuery q1 = new FilterQuery();
    q1.setId(1);
    q1.setColumn("dummy1");
    q1.addToValue("dummy1");
    q1.addToNestedFilterQueryIds(2);
    q1.setOperator(FilterOperator.AND);
    FilterQuery q2 = new FilterQuery();
    q2.setId(2);
    q2.setColumn("dummy2");
    q2.addToValue("dummy2");
    q2.setOperator(FilterOperator.AND);

    FilterQueryMap map = new FilterQueryMap();
    map.putToFilterQueryMap(1, q1);
    map.putToFilterQueryMap(2, q2);
    req.setFilterQuery(q1);
    req.setFilterSubQueryMap(map);

    //Populate Aggregations
    AggregationInfo agg = new AggregationInfo();
    agg.setAggregationType("dummy1");
    agg.putToAggregationParams("key1", "dummy1");
    req.addToAggregationsInfo(agg);

    TSerializer normalSerializer = new TSerializer();
    TSerializer compactSerializer = new TSerializer(new TCompactProtocol.Factory());
    normalSerializer.serialize(req);
    compactSerializer.serialize(req);

//    int numRequests = 100000;
//    TimerContext t = MetricsHelper.startTimer();
//    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
//    //TSerializer serializer = new TSerializer();
//    //Compact : Size 183 , Serialization Latency : 0.03361ms
//    // Normal : Size 385 , Serialization Latency : 0.01144ms
//
//    for (int i = 0; i < numRequests; i++) {
//      try {
//        serializer.serialize(req);
//        //System.out.println(s3.length);
//        //break;
//      } catch (TException e) {
//        e.printStackTrace();
//      }
//    }
//    t.stop();
//    System.out.println("Latency is :" + (t.getLatencyMs() / (float) numRequests));
  }
}
