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
package com.linkedin.pinot.perf;

import java.io.File;
import java.util.HashMap;

import org.json.JSONObject;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.pql.parsers.PQLCompiler;


/**
 * Allows us to benchmark filter operator in isolation
 * USAGE FilterOperatorBenchmark <IndexRootDir> <Query>
 * @author kgopalak
 *
 */
public class FilterOperatorBenchmark {
  public static void main(String[] args) throws Exception {
    String rootDir = args[0];
    File[] segmentDirs = new File(rootDir).listFiles();
    String query = args[1];
    for (File indexSegmentDir : segmentDirs) {

      IndexSegmentImpl indexSegmentImpl = new IndexSegmentImpl(indexSegmentDir, ReadMode.heap);
      PQLCompiler compiler = new PQLCompiler(new HashMap<String, String[]>());
      JSONObject jsonObject = compiler.compile(query);
      BrokerRequest brokerRequest = RequestConverter.fromJSON(jsonObject);
      int runCount = 0;
      while (runCount < 2) {
        FilterPlanNode planNode = new FilterPlanNode(indexSegmentImpl, brokerRequest);
        Operator operator = planNode.run();
        operator.open();
        Block block = operator.nextBlock();
        BlockDocIdIterator iterator = block.getBlockDocIdSet().iterator();
        int docId;
        int count = 0;
        while ((docId = iterator.next()) != Constants.EOF) {
          //System.out.println(docId);
          count = count + 1;
        }
        System.out.println("Matched Count" + count);
        operator.close();
        runCount = runCount + 1;
      }
    }
  }
}
