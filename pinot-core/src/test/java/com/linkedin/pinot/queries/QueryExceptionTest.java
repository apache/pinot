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
package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.manager.config.FileBasedInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.FileBasedInstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.query.reduce.BrokerReduceService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryExceptionTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueriesSentinelTest.class);
  private static ReduceService<BrokerResponseNative> REDUCE_SERVICE = new BrokerReduceService();

  private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();
  private final String AVRO_DATA = "data/test_data-mv.avro";
  private static File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator + "QueriesSentinelTest");
  private static QueryExecutor QUERY_EXECUTOR;
  private static TestingServerPropertiesBuilder CONFIG_BUILDER;
  private String segmentName;

  @BeforeClass
  public void setup() throws Exception {
    TableDataManagerProvider.setServerMetrics(new ServerMetrics(new MetricsRegistry()));

    CONFIG_BUILDER = new TestingServerPropertiesBuilder("testTable");

    setupSegmentFor("testTable");

    final PropertiesConfiguration serverConf = CONFIG_BUILDER.build();
    serverConf.setDelimiterParsingDisabled(false);

    final FileBasedInstanceDataManager instanceDataManager = FileBasedInstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(new FileBasedInstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();

    System.out.println("************************** : " + new File(INDEX_DIR, "segment").getAbsolutePath());
    File segmentFile = new File(INDEX_DIR, "segment").listFiles()[0];
    segmentName = segmentFile.getName();
    final IndexSegment indexSegment = ColumnarSegmentLoader.load(segmentFile, ReadMode.heap);
    instanceDataManager.getTableDataManager("testTable");
    instanceDataManager.getTableDataManager("testTable").addSegment(indexSegment);

    QUERY_EXECUTOR = new ServerQueryExecutorV1Impl(false);
    QUERY_EXECUTOR.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager, new ServerMetrics(
        new MetricsRegistry()));
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void setupSegmentFor(String table) throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    INDEX_DIR.mkdir();

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), new File(INDEX_DIR,
            "segment"), "daysSinceEpoch", TimeUnit.DAYS, table);

    final SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();

    driver.init(config);
    driver.build();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }

  @Test
  public void testSingleQuery() throws RecognitionException, Exception {
    String query = "select count(*) from testTable where column1='24516187'";
    LOGGER.info("running  : " + query);
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final BrokerRequest brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(query);
    InstanceRequest instanceRequest = new InstanceRequest(1, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    instanceRequest.getSearchSegments().add(segmentName);
    QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
    final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(queryRequest);
    instanceResponseMap.clear();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponseNative brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
  }

  @Test
  public void testQueryParsingFailedQuery() throws RecognitionException, Exception {
    String query = "select sudm(blablaa) from testTable where column1='24516187'";
    LOGGER.info("running  : " + query);
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final BrokerRequest brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(query);
    InstanceRequest instanceRequest = new InstanceRequest(1, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    instanceRequest.getSearchSegments().add(segmentName);
    QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
    final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(queryRequest);
    instanceResponseMap.clear();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponseNative brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.info("BrokerResponse is {}", brokerResponse);
    Assert.assertTrue(brokerResponse.getExceptionsSize() > 0);
  }

  @Test
  public void testQueryPlanFailedQuery() throws RecognitionException, Exception {
    String query = "select sum(blablaa) from testTable where column1='24516187'";
    LOGGER.info("running  : " + query);
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final BrokerRequest brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(query);
    InstanceRequest instanceRequest = new InstanceRequest(1, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    instanceRequest.getSearchSegments().add(segmentName);
    QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
    final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(queryRequest);
    instanceResponseMap.clear();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponseNative brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.info("BrokerResponse is {}", brokerResponse);
    Assert.assertTrue(brokerResponse.getExceptionsSize() > 0);
  }

  @Test
  public void testQueryExecuteFailedQuery() throws RecognitionException, Exception {
    String query = "select count(*) from testTable where column1='24516187' group by bla";
    LOGGER.info("running  : " + query);
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final BrokerRequest brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(query);
    InstanceRequest instanceRequest = new InstanceRequest(1, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    instanceRequest.getSearchSegments().add(segmentName);
    QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
    final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(queryRequest);
    instanceResponseMap.clear();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponseNative brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.info("BrokerResponse is {}", brokerResponse);
    Assert.assertEquals(brokerResponse.getExceptionsSize(), 1);
  }
}
