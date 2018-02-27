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
package com.linkedin.pinot.perf;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Level;

/**
 * Allows us to benchmark filter operator in isolation
 * USAGE FilterOperatorBenchmark &lt;IndexRootDir&gt; &lt;Query&gt;
 */
public class FilterOperatorBenchmark {
  static {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
  }

  public static void main(String[] args) throws Exception {
    String rootDir = args[0];
    File[] segmentDirs = new File(rootDir).listFiles();
    String query = args[1];
    AtomicInteger totalDocsMatched = new AtomicInteger(0);
    Pql2Compiler pql2Compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = pql2Compiler.compileToBrokerRequest(query);
    List<Callable<Void>> segmentProcessors = new ArrayList<>();
    long[] timesSpent = new long[segmentDirs.length];
    for (int i = 0; i < segmentDirs.length; i++) {
      File indexSegmentDir = segmentDirs[i];
      System.out.println("Loading " + indexSegmentDir.getName());

      Set<String> invertedColumns = new HashSet<>();
      String[] indexFiles = indexSegmentDir.list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".bitmap.inv");
        }
      });
      for (String indexFileName : indexFiles) {
        invertedColumns.add(indexFileName.replace(".bitmap.inv", ""));
      }
      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
      indexLoadingConfig.setReadMode(ReadMode.heap);
      indexLoadingConfig.setInvertedIndexColumns(invertedColumns);

      IndexSegmentImpl indexSegmentImpl =
          (IndexSegmentImpl) ColumnarSegmentLoader.load(indexSegmentDir, indexLoadingConfig);
      segmentProcessors.add(new SegmentProcessor(i, indexSegmentImpl, brokerRequest,
          totalDocsMatched, timesSpent));
    }
    ExecutorService executorService = Executors.newCachedThreadPool();
    for (int run = 0; run < 5; run++) {
      System.out.println("START RUN:"+ run );
      totalDocsMatched.set(0);
      long start = System.currentTimeMillis();
      List<Future<Void>> futures = executorService.invokeAll(segmentProcessors);
      for (int i = 0; i < futures.size(); i++) {
        futures.get(i).get();
      }
      long end = System.currentTimeMillis();
      System.out.println("Total docs matched:" + totalDocsMatched + " took:" + (end - start));
      System.out.println("Times spent:" + Arrays.toString(timesSpent));
      System.out.println("END RUN:"+ run );
    }
    System.exit(0);
  }

  public static class SegmentProcessor implements Callable<Void> {
    private IndexSegment indexSegmentImpl;
    private BrokerRequest brokerRequest;
    AtomicInteger totalDocsMatched;
    private long[] timesSpent;
    private int id;

    public SegmentProcessor(int id, IndexSegment indexSegmentImpl, BrokerRequest brokerRequest,
         AtomicInteger totalDocsMatched, long[] timesSpent) {
      super();
      this.id = id;
      this.indexSegmentImpl = indexSegmentImpl;
      this.brokerRequest = brokerRequest;
      this.totalDocsMatched = totalDocsMatched;
      this.timesSpent = timesSpent;
    }

    int[] docIds = new int[10000];
    int[] dictIds = new int[10000];
    long[] values = new long[10000];

    public Void call() {
      long start, end;
      start = System.currentTimeMillis();
      FilterPlanNode planNode = new FilterPlanNode(indexSegmentImpl, brokerRequest);
      Operator filterOperator = planNode.run();
      Block block = filterOperator.nextBlock();
      BlockDocIdSet filteredDocIdSet = block.getBlockDocIdSet();
      BlockDocIdIterator iterator = filteredDocIdSet.iterator();
      int docId;
      int matchedCount = 0;
      while ((docId = iterator.next()) != Constants.EOF) {
        matchedCount = matchedCount + 1;
        /* Sample ode to print a particular column from matched records
        {
          final String columnName = "someColumn";
          BlockSingleValIterator it =
              (BlockSingleValIterator) indexSegmentImpl.getDataSource(columnName).getNextBlock().getBlockValueSet()
                  .iterator();
          it.skipTo(docId);
          int dictId = it.nextIntVal();  // dict id
          // get the dictionary and use this dictionary id to get the value
          System.out.println("Segment " + indexSegmentImpl.getSegmentName() + " " + columnName + " " + indexSegmentImpl
              .getDataSource(columnName).getDictionary().get(dictId));
        }
        */
      }


      end = System.currentTimeMillis();
      timesSpent[id] = (end - start);
      totalDocsMatched.addAndGet(matchedCount);
      return null;
    }

  }
}
