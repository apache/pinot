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
package org.apache.pinot.tools.scan.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScanBasedQueryProcessor implements Cloneable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScanBasedQueryProcessor.class);

  Map<File, SegmentQueryProcessor> _segmentQueryProcessorMap;
  private long _timeoutInSeconds = 10000;
  private boolean _ownsQueryProcessors = false;

  private ScanBasedQueryProcessor() {
  }

  public ScanBasedQueryProcessor(String segmentsDirName)
      throws Exception {
    File segmentsDir = new File(segmentsDirName);
    _segmentQueryProcessorMap = new HashMap<>();
    _ownsQueryProcessors = true;

    for (File segmentFile : segmentsDir.listFiles()) {
      _segmentQueryProcessorMap.put(segmentFile, new SegmentQueryProcessor(segmentFile));
    }
  }

  public QueryResponse processQuery(String query)
      throws Exception {
    long startTimeInMillis = System.currentTimeMillis();
    Pql2Compiler pql2Compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = pql2Compiler.compileToBrokerRequest(query);
    ResultTable results = null;

    Aggregation aggregation = null;
    List<String> groupByColumns;
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo != null) {
      GroupBy groupBy = brokerRequest.getGroupBy();
      groupByColumns = (brokerRequest.isSetGroupBy()) ? groupBy.getExpressions() : null;
      long topN = (groupByColumns != null) ? groupBy.getTopN() : 10;
      aggregation = new Aggregation(brokerRequest.getAggregationsInfo(), groupByColumns, topN);
    }

    int numDocsScanned = 0;
    int totalDocs = 0;
    int numSegments = 0;
    LOGGER.info("Processing Query: {}", query);

    List<ResultTable> resultTables = processSegments(query, brokerRequest);
    for (ResultTable segmentResults : resultTables) {
      numDocsScanned += segmentResults.getNumDocsScanned();
      totalDocs += segmentResults.getTotalDocs();
      numSegments++;
      results = (results == null) ? segmentResults : results.append(segmentResults);
    }

    if (aggregation != null && numSegments > 1 && numDocsScanned > 0) {
      results = aggregation.aggregate(results);
    }

    if (results != null) {
      results.setNumDocsScanned(numDocsScanned);
      results.setTotalDocs(totalDocs);
      long totalUsedMs = System.currentTimeMillis() - startTimeInMillis;
      results.setProcessingTime(totalUsedMs);
      results.seal();
    }

    QueryResponse queryResponse = new QueryResponse(results);

    return queryResponse;
  }

  public void close() {
    if (_ownsQueryProcessors) {
      for (SegmentQueryProcessor segmentQueryProcessor : _segmentQueryProcessorMap.values()) {
        segmentQueryProcessor.close();
      }
    }
  }

  @Override
  public ScanBasedQueryProcessor clone() {
    ScanBasedQueryProcessor copy = new ScanBasedQueryProcessor();
    copy._segmentQueryProcessorMap = _segmentQueryProcessorMap;
    copy._ownsQueryProcessors = false;
    return copy;
  }

  private List<ResultTable> processSegments(final String query, final BrokerRequest brokerRequest)
      throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    final List<ResultTable> resultTables = Collections.synchronizedList(new ArrayList<ResultTable>());

    for (final SegmentQueryProcessor segmentQueryProcessor : _segmentQueryProcessorMap.values()) {
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            ResultTable resultTable = segmentQueryProcessor.process(brokerRequest);
            if (resultTable != null) {
              resultTables.add(resultTable);
            }
          } catch (Exception e) {
            LOGGER.error("Exception caught while processing segment '{}'.", segmentQueryProcessor.getSegmentName(), e);
            return;
          }
        }
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(_timeoutInSeconds, TimeUnit.SECONDS);
    return resultTables;
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 3) {
      LOGGER.error("Incorrect arguments");
      LOGGER.info("Usage: <exec> <UntarredSegmentDir> <QueryFile> <outputFile>");
      System.exit(1);
    }

    String segDir = args[0];
    String queryFile = args[1];
    String outputFile = args[2];
    String query;

    ScanBasedQueryProcessor scanBasedQueryProcessor = new ScanBasedQueryProcessor(segDir);
    BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile));
    PrintWriter printWriter = new PrintWriter(outputFile);

    while ((query = bufferedReader.readLine()) != null) {
      QueryResponse queryResponse = scanBasedQueryProcessor.processQuery(query);
      printResult(queryResponse);
      printWriter.println("Query: " + query);
      printWriter.println("Result: " + JsonUtils.objectToString(queryResponse));
    }
    bufferedReader.close();
    printWriter.close();
  }

  public static void printResult(QueryResponse queryResponse)
      throws IOException {
    LOGGER.info(JsonUtils.objectToString(queryResponse));
  }
}
