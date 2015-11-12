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
import java.util.HashMap;
import java.util.List;
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
    PQLCompiler compiler = new PQLCompiler(new HashMap<String, String[]>());
    JSONObject jsonObject = compiler.compile(query);
    BrokerRequest brokerRequest = RequestConverter.fromJSON(jsonObject);

    ResultTable results = null;
    File file = new File(_segmentsDir);

    Aggregation aggregation = null;
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo != null) {
      aggregation = new Aggregation(brokerRequest.getAggregationsInfo());
    }

    int numDocsScanned = 0;
    for (File segmentDir : file.listFiles()) {
      SegmentQueryProcessor processor = new SegmentQueryProcessor(brokerRequest, segmentDir);
      ResultTable segmentResults = processor.process(query);
      numDocsScanned += segmentResults.getNumDocsScanned();
      results = (results == null) ? segmentResults : results.append(segmentResults);
    }

    if (aggregation != null) {
      results = aggregation.aggregate(results);
    }

    results.setNumDocsScanned(numDocsScanned);
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
      LOGGER.info("Processing Query: {}", query);
      ResultTable resultTable = scanBasedQueryProcessor.processQuery(query);
      if (resultTable != null) {
        resultTable.print();
      }
    }
    bufferedReader.close();
  }
}
