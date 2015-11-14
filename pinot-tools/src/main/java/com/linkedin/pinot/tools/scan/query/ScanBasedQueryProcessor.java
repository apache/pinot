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
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.pql.parsers.PQLCompiler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScanBasedQueryProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScanBasedQueryProcessor.class);
  private final String _segmentsDir;

  public ScanBasedQueryProcessor(String segmentsDir) {
    _segmentsDir = segmentsDir;
  }

  ResultTable processQuery(String query)
      throws Exception {
    long startTimeInMillis = System.currentTimeMillis();
    PQLCompiler compiler = new PQLCompiler(new HashMap<String, String[]>());
    JSONObject jsonObject = compiler.compile(query);
    BrokerRequest brokerRequest = RequestConverter.fromJSON(jsonObject);

    ResultTable results = null;
    File file = new File(_segmentsDir);

    Aggregation aggregation = null;
    List<String> groupByColumns = null;
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo != null) {
      groupByColumns = (brokerRequest.isSetGroupBy()) ? brokerRequest.getGroupBy().getColumns() : null;
      aggregation = new Aggregation(brokerRequest.getAggregationsInfo(), groupByColumns);
    }

    int numDocsScanned = 0;
    int totalDocs = 0;
    LOGGER.info("Processing Query: {}", query);

    for (File segmentDir : file.listFiles()) {
      LOGGER.info("Processing segment: " + segmentDir.getName());
      SegmentQueryProcessor processor = new SegmentQueryProcessor(brokerRequest, segmentDir);
      ResultTable segmentResults = processor.process(query);
      numDocsScanned += segmentResults.getNumDocsScanned();
      totalDocs += segmentResults.getTotalDocs();
      results = (results == null) ? segmentResults : results.append(segmentResults);
    }

    if (aggregation != null) {
      results = aggregation.aggregate(results);
    }

    results.setNumDocsScanned(numDocsScanned);
    results.setTotalDocs(totalDocs);
    long totalUsedMs = System.currentTimeMillis() - startTimeInMillis;
    results.setProcessingTime(totalUsedMs);

    printResult(results);
    return results;
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 2) {
      LOGGER.error("Incorrect arguments");
      LOGGER.info("Usage: <exec> <UntarredSegmentDir> <QueryFile");
      System.exit(1);
    }

    String segDir = args[0];
    String queryFile = args[1];
    String query;

    ScanBasedQueryProcessor scanBasedQueryProcessor = new ScanBasedQueryProcessor(segDir);
    BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile));

    while ((query = bufferedReader.readLine()) != null) {
      scanBasedQueryProcessor.processQuery(query);
    }
    bufferedReader.close();
  }

  private void printResult(ResultTable resultTable)
      throws IOException {
    List<String> values = new ArrayList<>();

    for (ResultTable.Row row : resultTable) {
      int columnId = 0;
      for (Object value : row) {
        values.add(row.get(columnId).toString());
      }
    }

    Result result =
        new Result(resultTable.getNumDocsScanned(), resultTable.getTotalDocs(), resultTable.getProcessingTime(), values);
    ObjectMapper objectMapper = new ObjectMapper();
    LOGGER.info(objectMapper.defaultPrettyPrintingWriter().writeValueAsString(result));
  }

  private class Result {
    private int _numDocsScanned;
    private int _totalDocs;
    private long _timeUsedMs;
    List<String> _aggregationResults;

    Result(int numDocsScanned, int totalDocs, long timeUsedMs, List<String> aggregationResults) {
      _numDocsScanned = numDocsScanned;
      _totalDocs = totalDocs;
      _timeUsedMs = timeUsedMs;
      _aggregationResults = aggregationResults;
    }

    public int getNumDocsScanned() {
      return _numDocsScanned;
    }

    public int getTotalDocs() {
      return _totalDocs;
    }

    public long getTimeUsedMs() {
      return _timeUsedMs;
    }

    List<String> getAggregationResults() {
      return _aggregationResults;
    }
  }
}
