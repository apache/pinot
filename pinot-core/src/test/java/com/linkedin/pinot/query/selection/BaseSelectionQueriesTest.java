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
package com.linkedin.pinot.query.selection;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOnlyOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOrderByOperator;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;
import com.linkedin.pinot.core.query.selection.SelectionOperatorUtils;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


/**
 * The <code>BaseSelectionQueriesTest</code> class is the base class for the selection queries tests.
 * <p>By overriding the abstract methods, this class will test the following parts:
 * <ul>
 *   <li>Iterate on data set for both selection ONLY and selection ORDER BY queries.</li>
 *   <li>Reduce on selection results for both selection ONLY and selection ORDER BY queries.</li>
 *   <li>Use inner segment plan for both selection ONLY and selection ORDER BY queries.</li>
 * </ul>
 */
public abstract class BaseSelectionQueriesTest {

  /**
   * Get the {@link IndexSegment} for the tests.
   *
   * @return index segment for the tests.
   */
  abstract IndexSegment getIndexSegment();

  /**
   * Get the {@link Map} from server instance to {@link DataSource}.
   *
   * @return data source map for the tests.
   */
  abstract Map<String, BaseOperator> getDataSourceMap();

  /**
   * Get the selection ONLY query.
   *
   * @return selection ONLY query.
   */
  abstract Selection getSelectionOnlyQuery();

  /**
   * Get the selection ORDER BY query.
   *
   * @return selection ORDER BY query.
   */
  abstract Selection getSelectionOrderByQuery();

  /**
   * Verify the result for selection ONLY query.
   *
   * @param selectionOnlyResult result for selection ONLY query.
   */
  abstract void verifySelectionOnlyResult(Collection<Serializable[]> selectionOnlyResult);

  /**
   * Verify the result for selection ORDER BY query.
   *
   * @param selectionOrderByResult result for selection ORDER BY query.
   */
  abstract void verifySelectionOrderByResult(Collection<Serializable[]> selectionOrderByResult);

  /**
   * Verify the reduced result for selection ORDER BY query.
   *
   * @param reducedSelectionOrderByResult reduced result for selection ORDER BY query.
   */
  abstract void verifyReducedSelectionOrderByResult(Collection<Serializable[]> reducedSelectionOrderByResult);

  /**
   * This method tests iterating on data set and reducing on selection results for selection ONLY queries.
   *
   * @throws Exception
   */
  @Test
  public void testSelectionOnlyIteration()
      throws Exception {
    IndexSegment indexSegment = getIndexSegment();
    Map<String, BaseOperator> dataSourceMap = getDataSourceMap();
    Selection selectionOnlyQuery = getSelectionOnlyQuery();

    // Get selection result block.
    int numTotalDocs = indexSegment.getSegmentMetadata().getTotalDocs();
    Operator filterOperator = new MatchEntireSegmentOperator(numTotalDocs);
    BReusableFilteredDocIdSetOperator docIdSetOperator =
        new BReusableFilteredDocIdSetOperator(filterOperator, numTotalDocs, 5000);
    MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);
    MSelectionOnlyOperator selectionOperator =
        new MSelectionOnlyOperator(indexSegment, selectionOnlyQuery, projectionOperator);
    IntermediateResultsBlock block = (IntermediateResultsBlock) selectionOperator.nextBlock();
    Collection<Serializable[]> selectionResult = block.getSelectionResult();

    // Verify selection result.
    verifySelectionOnlyResult(selectionResult);

    // Verify reduced selection result. This step reduces 10 identical selection results.
    DataTable dataTable = block.getDataTable();
    Map<ServerInstance, DataTable> selectionResults = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      selectionResults.put(new ServerInstance("localhost:" + i), dataTable);
    }
    Collection<Serializable[]> reducedSelectionResult =
        SelectionOperatorUtils.reduceWithoutOrdering(selectionResults, 10);
    verifySelectionOnlyResult(reducedSelectionResult);
  }

  /**
   * This method tests iterating on data set and reducing on selection results for selection ORDER BY queries.
   *
   * @throws Exception
   */
  @Test
  public void testSelectionOrderByIteration()
      throws Exception {
    IndexSegment indexSegment = getIndexSegment();
    Map<String, BaseOperator> dataSourceMap = getDataSourceMap();
    Selection selectionOrderByQuery = getSelectionOrderByQuery();

    // Get selection result block.
    int numTotalDocs = indexSegment.getSegmentMetadata().getTotalDocs();
    Operator filterOperator = new MatchEntireSegmentOperator(numTotalDocs);
    BReusableFilteredDocIdSetOperator docIdSetOperator =
        new BReusableFilteredDocIdSetOperator(filterOperator, numTotalDocs, 5000);
    MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);
    MSelectionOrderByOperator selectionOperator =
        new MSelectionOrderByOperator(indexSegment, selectionOrderByQuery, projectionOperator);
    IntermediateResultsBlock block = (IntermediateResultsBlock) selectionOperator.nextBlock();

    // Verify selection result.
    Collection<Serializable[]> selectionResult = block.getSelectionResult();
    verifySelectionOrderByResult(selectionResult);

    // Verify reduced selection result. This step reduces 10 identical selection results.
    DataTable dataTable = block.getDataTable();
    Map<ServerInstance, DataTable> selectionResults = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      selectionResults.put(new ServerInstance("localhost:" + i), dataTable);
    }
    Collection<Serializable[]> reducedSelectionResult =
        new SelectionOperatorService(selectionOrderByQuery ,indexSegment).reduceWithOrdering(selectionResults);
    verifyReducedSelectionOrderByResult(reducedSelectionResult);
  }

  /**
   * This method tests using inner segment plan for selection ONLY queries.
   */
  @Test
  public void testSelectionOnlyInnerSegmentPlan() {
    IndexSegment indexSegment = getIndexSegment();
    Selection selectionOnlyQuery = getSelectionOnlyQuery();

    // Use inner segment plan to get selection result.
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(selectionOnlyQuery);
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode planNode = instancePlanMaker.makeInnerSegmentPlan(indexSegment, brokerRequest);
    MSelectionOnlyOperator selectionOperator = (MSelectionOnlyOperator) planNode.run();
    IntermediateResultsBlock block = (IntermediateResultsBlock) selectionOperator.nextBlock();
    Collection<Serializable[]> selectionResult = block.getSelectionResult();

    // Verify selection result.
    verifySelectionOnlyResult(selectionResult);
  }

  /**
   * This method tests using inner segment plan for selection ORDER BY queries.
   */
  @Test
  public void testSelectionOrderByInnerSegmentPlan() {
    IndexSegment indexSegment = getIndexSegment();
    Selection selectionOrderByQuery = getSelectionOrderByQuery();

    // Use inner segment plan to get selection result.
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(selectionOrderByQuery);
    PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    PlanNode planNode = instancePlanMaker.makeInnerSegmentPlan(indexSegment, brokerRequest);
    MSelectionOrderByOperator selectionOperator = (MSelectionOrderByOperator) planNode.run();
    IntermediateResultsBlock block = (IntermediateResultsBlock) selectionOperator.nextBlock();
    Collection<Serializable[]> selectionResult = block.getSelectionResult();

    // Verify selection result.
    verifySelectionOrderByResult(selectionResult);
  }
}
