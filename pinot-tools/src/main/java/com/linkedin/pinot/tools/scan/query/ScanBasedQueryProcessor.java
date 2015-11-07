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
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.pql.parsers.PQLCompiler;
import java.io.File;
import java.util.HashMap;
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
    for (File segmentDir : file.listFiles()) {
      SegmentQueryProcessor processor = new SegmentQueryProcessor(brokerRequest, segmentDir);
      ResultTable segmentResults = processor.process(query);
      //ResultTable result = Utils.aggregate(projectionResult, aggColumns, aggFunctions);
    }

    return results;
  }

  public static void main(String[] args)
      throws Exception {
    ScanBasedQueryProcessor scanBasedQueryProcessor = new ScanBasedQueryProcessor("/Users/mshrivas/quick/data/pinotSegments");
    scanBasedQueryProcessor.processQuery("select ArrDelay, min(ArrTime), max(DepTime) from myTable where Origin = 'JFK'");
  }
}
