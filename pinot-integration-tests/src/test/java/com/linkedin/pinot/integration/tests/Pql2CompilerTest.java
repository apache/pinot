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
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.pql.parsers.PQLCompiler;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.Collections;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Test for the PQL2 compiler that compares behavior with the old parser.
 */
public class Pql2CompilerTest {
  private long pql1Nanos = 0L;
  private long pql2Nanos = 0L;
  private int queryCount = 0;

  private void testQuery(PQLCompiler pql1Compiler, Pql2Compiler pql2Compiler, String pql) {
    try {
      //System.out.println(pql);
      long pql1StartTime = System.nanoTime();
      // Skip ones that don't compile with Pinot 1
      JSONObject jsonObject;
      try {
        jsonObject = pql1Compiler.compile(pql);
      } catch (Exception e) {
        return;
      }

      BrokerRequest pqlBrokerRequest = RequestConverter.fromJSON(jsonObject);
      pql1Nanos += (System.nanoTime() - pql1StartTime);
      queryCount++;
      long pql2StartTime = System.nanoTime();
      BrokerRequest pql2BrokerRequest = pql2Compiler.compileToBrokerRequest(pql);
      pql2Nanos += (System.nanoTime() - pql2StartTime);
      Assert.assertTrue(brokerRequestsAreEquivalent(pqlBrokerRequest, pql2BrokerRequest),
          "Requests are not equivalent\npql2: " + pql2BrokerRequest + "\npql: " + pqlBrokerRequest + "\nquery:" + pql);
    } catch (Exception e) {
      Assert.fail("Caught exception compiling " + pql, e);
    }
  }

  @BeforeMethod
  public void setUp() {
    pql1Nanos = 0L;
    pql2Nanos = 0L;
    queryCount = 0;
  }

  @AfterMethod
  public void displayAverageNanoTime() {
    System.out.println(queryCount + " queries executed");
    System.out.println("PQL1: total " + (pql1Nanos / 1000000.0) + "ms, avg " + ((double)pql1Nanos) / (1000000.0 * queryCount) + "ms");
    System.out.println(
        "PQL2: total " + (pql2Nanos / 1000000.0) + "ms, avg " + ((double) pql2Nanos) / (1000000.0 * queryCount) + "ms");
  }

  @Test
  public void testHardcodedQueries() {
    PQLCompiler pql1Compiler = new PQLCompiler(new HashMap<>());
    Pql2Compiler pql2Compiler = new Pql2Compiler();

    testQuery(pql1Compiler, pql2Compiler, "select count(*) from foo where x not in (1,2,3)");
  }
  
  @Test
  public void testGeneratedQueries() throws Exception {
    final File tempDir = new File("/tmp/Pql2CompilerTest");
    tempDir.mkdirs();

    TarGzCompressionUtils.unTar(new File(TestUtils.getFileFromResourceUrl(
        OfflineClusterIntegrationTest.class.getClassLoader()
            .getResource("On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz"))), tempDir);

    try {
      File avroFile = new File(tempDir, "On_Time_On_Time_Performance_2014_1.avro");
      QueryGenerator qg = new QueryGenerator(Collections.singletonList(avroFile), "whatever", "whatever");

      PQLCompiler pql1Compiler = new PQLCompiler(new HashMap<>());
      Pql2Compiler pql2Compiler = new Pql2Compiler();

      for (int i = 1; i <= 1000000; i++) {
        String pql = qg.generateQuery().generatePql();
        testQuery(pql1Compiler, pql2Compiler, pql);
      }
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  private boolean brokerRequestsAreEquivalent(BrokerRequest left, BrokerRequest right) {
    boolean queryTypeIsEqual = EqualityUtils.isEqual(left.getQueryType(), right.getQueryType());
    boolean querySourceIsEqual = EqualityUtils.isEqual(left.getQuerySource(), right.getQuerySource());
    boolean timeInterlalIsEqual = EqualityUtils.isEqual(left.getTimeInterval(), right.getTimeInterval());
    boolean durationIsEqual = EqualityUtils.isEqual(left.getDuration(), right.getDuration());
    boolean selectionsAreEqual = EqualityUtils.isEqual(left.getSelections(), right.getSelections());
    boolean bucketHashKeyIsEqual = EqualityUtils.isEqual(left.getBucketHashKey(), right.getBucketHashKey());
    boolean basicFieldsAreEquivalent = queryTypeIsEqual &&
        querySourceIsEqual &&
        timeInterlalIsEqual &&
        durationIsEqual &&
        selectionsAreEqual &&
        bucketHashKeyIsEqual;

    boolean aggregationsAreEquivalent = true;

    List<AggregationInfo> leftAggregationsInfo = left.getAggregationsInfo();
    List<AggregationInfo> rightAggregationsInfo = right.getAggregationsInfo();
    if (!EqualityUtils.isEqual(leftAggregationsInfo, rightAggregationsInfo)) {
      if (leftAggregationsInfo == null || rightAggregationsInfo == null ||
          leftAggregationsInfo.size() != rightAggregationsInfo.size()) {
        aggregationsAreEquivalent = false;
      } else {
        ArrayList<AggregationInfo> leftAggregationsInfoCopy = new ArrayList<>(leftAggregationsInfo);
        ArrayList<AggregationInfo> rightAggregationsInfoCopy = new ArrayList<>(rightAggregationsInfo);
        int aggregationsInfoCount = leftAggregationsInfoCopy.size();
        for (int i = 0; i < aggregationsInfoCount; i++) {
          AggregationInfo leftInfo = leftAggregationsInfoCopy.get(i);

          for (int j = 0; j < rightAggregationsInfoCopy.size(); ++j) {
            AggregationInfo rightInfo = rightAggregationsInfoCopy.get(j);

            // Check if the aggregationsInfo are the same or they're the count function
            if (EqualityUtils.isEqual(leftInfo, rightInfo)) {
              rightAggregationsInfoCopy.remove(j);
              break;
            } else {
              if ("count".equalsIgnoreCase(rightInfo.getAggregationType()) && "count"
                  .equalsIgnoreCase(leftInfo.getAggregationType())) {
                rightAggregationsInfoCopy.remove(j);
                break;
              }
            }
          }
        }

        aggregationsAreEquivalent = rightAggregationsInfoCopy.isEmpty();
      }
    }

    // Group by clauses might not be in the same order
    boolean groupByClauseIsEquivalent = EqualityUtils.isEqual(left.getGroupBy(), right.getGroupBy());

    if (!groupByClauseIsEquivalent) {
      groupByClauseIsEquivalent =
          (EqualityUtils.isEqualIgnoringOrder(left.getGroupBy().getColumns(), right.getGroupBy().getColumns()) &&
              EqualityUtils.isEqual(left.getGroupBy().getTopN(), right.getGroupBy().getTopN()));
    }

    boolean filtersAreEquivalent = EqualityUtils.isEqual(left.isSetFilterQuery(), right.isSetFilterQuery());

    if (left.isSetFilterQuery()) {
      int leftRootId = left.getFilterQuery().getId();
      int rightRootId = right.getFilterQuery().getId();
      // The Pql 1 compiler merges ranges, the Pql 2 compiler doesn't, so we skip the filter comparison if either side
      // has more than one range filter for the same column
      filtersAreEquivalent = !filtersHaveAtMostOneRangeFilterPerColumn(left, right) || filterQueryIsEquivalent(
          Collections.singletonList(leftRootId), Collections.singletonList(rightRootId), left.getFilterSubQueryMap(),
          right.getFilterSubQueryMap());
    }

    boolean areEqual =
        basicFieldsAreEquivalent && aggregationsAreEquivalent && groupByClauseIsEquivalent && filtersAreEquivalent;

    if (!areEqual) {
      System.out.println("queryTypeIsEqual = " + queryTypeIsEqual);
      System.out.println("querySourceIsEqual = " + querySourceIsEqual);
      System.out.println("timeInterlalIsEqual = " + timeInterlalIsEqual);
      System.out.println("durationIsEqual = " + durationIsEqual);
      System.out.println("selectionsAreEqual = " + selectionsAreEqual);
      System.out.println("bucketHashKeyIsEqual = " + bucketHashKeyIsEqual);
      System.out.println("basicFieldsAreEquivalent = " + basicFieldsAreEquivalent);
      System.out.println("aggregationsAreEquivalent = " + aggregationsAreEquivalent);
      System.out.println("groupByClauseIsEquivalent = " + groupByClauseIsEquivalent);
      System.out.println("filtersAreEquivalent = " + filtersAreEquivalent);
      
      if (!filtersAreEquivalent) {
        int leftRootId = left.getFilterQuery().getId();
        int rightRootId = right.getFilterQuery().getId();
        displayFilterDifference(Collections.singletonList(leftRootId), Collections.singletonList(rightRootId),
            left.getFilterSubQueryMap(), right.getFilterSubQueryMap());
      }
    }

    return areEqual;
  }

  private void displayFilterDifference(List<Integer> leftIds, List<Integer> rightIds, FilterQueryMap leftSubQueries,
      FilterQueryMap rightSubQueries) {
    ArrayList<Integer> leftIdsCopy = new ArrayList<>(leftIds);
    ArrayList<Integer> rightIdsCopy = new ArrayList<>(rightIds);

    Iterator<Integer> leftIterator = leftIdsCopy.iterator();
    while (leftIterator.hasNext()) {
      Integer leftId = leftIterator.next();

      Iterator<Integer> rightIterator = rightIdsCopy.iterator();
      while (rightIterator.hasNext()) {
        Integer rightId = rightIterator.next();
        if (filterQueryIsEquivalent(Collections.singletonList(leftId), Collections.singletonList(rightId), leftSubQueries, rightSubQueries)) {
          leftIterator.remove();
          rightIterator.remove();
          break;
        }
      }
    }

    if (!leftIdsCopy.isEmpty()) {
      System.out.println(" ----- ");
      for (Integer leftId : leftIdsCopy) {
        System.out.println("leftSubQuery = " + leftSubQueries.getFilterQueryMap().get(leftId));
      }

      for (Integer rightId : rightIdsCopy) {
        System.out.println("rightSubQuery = " + rightSubQueries.getFilterQueryMap().get(rightId));
      }
      System.out.println(" ----- ");



      if (leftIdsCopy.size() != 1 || rightIdsCopy.size() != 1) {
        System.out.println("MORE THAN ONE DIFFERENCE!");
      } else {
        int leftId = leftIdsCopy.get(0);
        int rightId = rightIdsCopy.get(0);
        FilterQuery left = leftSubQueries.getFilterQueryMap().get(leftId);
        FilterQuery right = rightSubQueries.getFilterQueryMap().get(rightId);
        ArrayList<Integer> leftChildrenIdsCopy = new ArrayList<>(left.getNestedFilterQueryIds());
        ArrayList<Integer> rightChildrenIdsCopy = new ArrayList<>(right.getNestedFilterQueryIds());

        Iterator<Integer> leftChildrenIterator = leftChildrenIdsCopy.iterator();
        while (leftChildrenIterator.hasNext()) {
          Integer leftChildrenId = leftChildrenIterator.next();

          Iterator<Integer> rightChildrenIterator = rightChildrenIdsCopy.iterator();
          while (rightChildrenIterator.hasNext()) {
            Integer rightChildrenId = rightChildrenIterator.next();
            if (filterQueryIsEquivalent(Collections.singletonList(leftChildrenId), Collections.singletonList(rightChildrenId), leftSubQueries, rightSubQueries)) {
              leftChildrenIterator.remove();
              rightChildrenIterator.remove();
              break;
            }
          }
        }

        displayFilterDifference(leftChildrenIdsCopy, rightChildrenIdsCopy, leftSubQueries, rightSubQueries);
      }
    }
  }

  private boolean filtersHaveAtMostOneRangeFilterPerColumn(BrokerRequest left, BrokerRequest right) {
    Set<String> leftRangeFilterColumns = new HashSet<>();
    for (FilterQuery filterQuery : left.getFilterSubQueryMap().getFilterQueryMap().values()) {
      if (filterQuery.getOperator() == FilterOperator.RANGE) {
        String column = filterQuery.getColumn();
        if (leftRangeFilterColumns.contains(column)) {
          return false;
        } else {
          leftRangeFilterColumns.add(column);
        }
      }
    }

    Set<String> rightRangeFilterColumns = new HashSet<>();
    for (FilterQuery filterQuery : right.getFilterSubQueryMap().getFilterQueryMap().values()) {
      if (filterQuery.getOperator() == FilterOperator.RANGE) {
        String column = filterQuery.getColumn();
        if (rightRangeFilterColumns.contains(column)) {
          return false;
        } else {
          rightRangeFilterColumns.add(column);
        }
      }
    }

    return true;
  }

  private boolean filterQueryIsEquivalent(List<Integer> leftIds, List<Integer> rightIds,
      FilterQueryMap leftFilterQueries, FilterQueryMap rightFilterQueries) {
    ArrayList<Integer> leftIdsCopy = new ArrayList<>(leftIds);
    ArrayList<Integer> rightIdsCopy = new ArrayList<>(rightIds);


    if (leftIdsCopy.size() != rightIdsCopy.size()) {
      return false;
    }

    Iterator<Integer> leftIterator = leftIdsCopy.iterator();

    while (leftIterator.hasNext()) {
      Integer leftId = leftIterator.next();
      FilterQuery leftQuery = leftFilterQueries.getFilterQueryMap().get(leftId);

      Iterator<Integer> rightIterator = rightIdsCopy.iterator();
      while (rightIterator.hasNext()) {
        Integer rightId = rightIterator.next();
        FilterQuery rightQuery = rightFilterQueries.getFilterQueryMap().get(rightId);

        boolean operatorsAreEqual = EqualityUtils.isEqual(leftQuery.getOperator(), rightQuery.getOperator());
        boolean columnsAreEqual = EqualityUtils.isEqual(leftQuery.getColumn(), rightQuery.getColumn());
        boolean valuesAreEqual = EqualityUtils.isEqual(leftQuery.getValue(), rightQuery.getValue());
        boolean fieldsAreEqual = columnsAreEqual &&
            operatorsAreEqual &&
            valuesAreEqual;

        // Compare sets if the op is IN
        if (operatorsAreEqual && columnsAreEqual && leftQuery.getOperator() == FilterOperator.IN) {
          Set<String> leftValues = new HashSet<>(Arrays.asList(leftQuery.getValue().get(0).split("\t\t")));
          Set<String> rightValues = new HashSet<>(Arrays.asList(rightQuery.getValue().get(0).split("\t\t")));
          fieldsAreEqual = leftValues.equals(rightValues);
          if (!fieldsAreEqual) {
            System.out.println("in clause not the same?");
            System.out.println("leftValues = " + leftValues);
            System.out.println("rightValues = " + rightValues);
          }
        }

        // NOT_IN and NOT are equivalent
        if (!operatorsAreEqual && columnsAreEqual && valuesAreEqual) {
          if (
              (leftQuery.getOperator() == FilterOperator.NOT || leftQuery.getOperator() == FilterOperator.NOT_IN) &&
                  (rightQuery.getOperator() == FilterOperator.NOT || rightQuery.getOperator() == FilterOperator.NOT_IN)
              ) {
            fieldsAreEqual = true;
          }
        }

        if (fieldsAreEqual) {
          if (filterQueryIsEquivalent(
              leftQuery.getNestedFilterQueryIds(),
              rightQuery.getNestedFilterQueryIds(),
              leftFilterQueries,
              rightFilterQueries
          )) {
            leftIterator.remove();
            rightIterator.remove();
            break;
          }
        }
      }
    }

    return leftIdsCopy.isEmpty();
  }
}
