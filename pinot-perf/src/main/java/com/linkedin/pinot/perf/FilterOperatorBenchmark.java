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
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.json.JSONObject;
import org.roaringbitmap.IntIterator;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.operator.blocks.UnSortedSingleValueBlock;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.pql.parsers.PQLCompiler;


/**
 * Allows us to benchmark filter operator in isolation
 * USAGE FilterOperatorBenchmark &lt;IndexRootDir&gt; &lt;Query&gt;
 *
 */
public class FilterOperatorBenchmark {
  static {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
  }

  public static void main(String[] args) throws Exception {
    String rootDir = args[0];
    File[] segmentDirs = new File(rootDir).listFiles();
    String query = args[1];

    AtomicLong[] globalSums;
    AtomicInteger totalDocsMatched = new AtomicInteger(0);
    PQLCompiler compiler = new PQLCompiler(new HashMap<String, String[]>());
    JSONObject jsonObject = compiler.compile(query);
    BrokerRequest brokerRequest = RequestConverter.fromJSON(jsonObject);
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();

    String[] columns = new String[aggregationsInfo.size()];
    globalSums = new AtomicLong[columns.length];
    for (int i = 0; i < aggregationsInfo.size(); i++) {
      AggregationInfo aggregationInfo = aggregationsInfo.get(i);
      String columnName = aggregationInfo.getAggregationParams().get("column");
      System.out.println(aggregationInfo.getAggregationType());
      columns[i] = columnName;
      globalSums[i] = new AtomicLong(0);
    }

    List<Callable<Void>> segmentProcessors = new ArrayList<>();
    long[] timesSpent = new long[segmentDirs.length];
    for (int i = 0; i < segmentDirs.length; i++) {
      File indexSegmentDir = segmentDirs[i];
      if (indexSegmentDir.getName().indexOf("_pricing") < 0) {
        continue;
      }
      System.out.println("Loading " + indexSegmentDir.getName());
      Configuration tableDataManagerConfig = new PropertiesConfiguration();
      List<String> invertedColumns = new ArrayList<>();
      FilenameFilter filter = new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".bitmap.inv");
        }
      };
      String[] indexFiles = indexSegmentDir.list(filter);
      for (String indexFileName : indexFiles) {
        invertedColumns.add(indexFileName.replace(".bitmap.inv", ""));
      }
      tableDataManagerConfig.setProperty(IndexLoadingConfigMetadata.KEY_OF_LOADING_INVERTED_INDEX, invertedColumns);
      IndexLoadingConfigMetadata indexLoadingConfigMetadata = new IndexLoadingConfigMetadata(tableDataManagerConfig);
      IndexSegmentImpl indexSegmentImpl =
          (IndexSegmentImpl) Loaders.IndexSegment.load(indexSegmentDir, ReadMode.heap, indexLoadingConfigMetadata);
      segmentProcessors.add(
          new SegmentProcessor(i, indexSegmentImpl, brokerRequest, columns, globalSums, totalDocsMatched, timesSpent));
    }
    ExecutorService executorService = Executors.newWorkStealingPool();
    for (int run = 0; run < 5; run++) {
      for (AtomicLong globalSum : globalSums) {
        globalSum.set(0);
      }
      totalDocsMatched.set(0);
      long start = System.currentTimeMillis();
      List<Future<Void>> futures = executorService.invokeAll(segmentProcessors);
      for (int i = 0; i < futures.size(); i++) {
        futures.get(i).get();
      }
      long end = System.currentTimeMillis();
      System.out.println(
          "Total docs matched:" + totalDocsMatched + " sum:" + Arrays.toString(globalSums) + " took:" + (end - start));
      System.out.println("Times spent:" + Arrays.toString(timesSpent));
    }
    System.exit(0);
  }

  public static class SegmentProcessor implements Callable<Void> {
    private IndexSegment indexSegmentImpl;
    private BrokerRequest brokerRequest;
    String columns[];
    AtomicLong[] globalSums;
    AtomicInteger totalDocsMatched;
    private long[] timesSpent;
    private int id;

    public SegmentProcessor(int id, IndexSegment indexSegmentImpl, BrokerRequest brokerRequest, String[] columns,
        AtomicLong[] globalSums, AtomicInteger totalDocsMatched, long[] timesSpent) {
      super();
      this.id = id;
      this.indexSegmentImpl = indexSegmentImpl;
      this.brokerRequest = brokerRequest;
      this.columns = columns;
      this.globalSums = globalSums;
      this.totalDocsMatched = totalDocsMatched;
      this.timesSpent = timesSpent;
    }
    int[] docIds = new int[10000];
    int[] dictIds = new int[10000];
    long[] values = new long[10000];

    public Void call() {
      long start, filterPhaseEnd, end;
      start = System.currentTimeMillis();
      FilterPlanNode planNode = new FilterPlanNode(indexSegmentImpl, brokerRequest);
      Operator filterOperator = planNode.run();
      filterOperator.open();
      Block block = filterOperator.nextBlock();
      BlockDocIdSet filteredDocIdSet = block.getBlockDocIdSet();
      BlockDocIdIterator iterator = filteredDocIdSet.iterator();
      filterPhaseEnd = System.currentTimeMillis();
      int docId;
      int matchedCount = 0;
      Map<String, BlockSingleValIterator> dataSourceMap = new HashMap<>();
      Map<String, UnSortedSingleValueBlock> unsortedSingleValueBlockMap = new HashMap<>();
      Map<String, Dictionary> dictionaryMap = new HashMap<>();
      for (int i = 0; i < columns.length; i++) {
        String column = columns[i];
        DataSource dataSource = indexSegmentImpl.getDataSource(column);
        Block unsortedSVBlock = dataSource.getNextBlock();
        dataSourceMap.put(column, (BlockSingleValIterator) unsortedSVBlock.getBlockValueSet().iterator());
        unsortedSingleValueBlockMap.put(column, (UnSortedSingleValueBlock) unsortedSVBlock);
        dictionaryMap.put(column, dataSource.getDictionary());
      }
      long[] sums = new long[dataSourceMap.size()];
      Arrays.fill(sums, 0);
      IntIterator docIdIterator = filteredDocIdSet.getRaw();
      boolean iteratorBased = true;

      if (iteratorBased) {
        while ((docId = iterator.next())!= Constants.EOF){
          matchedCount = matchedCount + 1;
//          for (int i = 0; i < columns.length; i++) {
//            String column = columns[i];
//            BlockSingleValIterator valIterator = dataSourceMap.get(column);
//            valIterator.skipTo(docId);
//            int dictionaryId = valIterator.nextIntVal();
//            //sums[i] += dictionaryMap.get(column).getLongValue(dictionaryId);
//            //sums[i] += valIterator.nextIntVal();
//          }
        }
      } else {
        boolean done = true;
        do {
          //collect 5k docIds
          int idx = 0;
          done = true;
          while (docIdIterator.hasNext()) {
            docIds[idx] = docIdIterator.next();
            idx = idx + 1;
            matchedCount = matchedCount + 1;
            if (idx == docIds.length - 1) {
              done = false;
              break;
            }
          }
          //read 5k dict Ids
          for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            UnSortedSingleValueBlock unSortedSingleValueBlock = unsortedSingleValueBlockMap.get(column);
            SingleColumnSingleValueReader svReader = unSortedSingleValueBlock.getSVReader();
            for (int j = 0; j < idx; j++) {
              dictIds[j] = svReader.getInt(docIds[j]);
            }

            //read 5k dictionary look ups
            Dictionary dictionary = dictionaryMap.get(column);
            for (int j = 0; j < idx; j++) {
              values[j] = dictionary.getLongValue(dictIds[j]);
            }
            //compute 5k sum
            for (int j = 0; j < idx; j++) {
              sums[i] += values[j];
            }
          }
        } while (!done);
      }
      end = System.currentTimeMillis();
      timesSpent[id] = (end - start);
      //System.out.println("Matched Count:" + matchedCount + " time:" + (end - start) + " sums:" + Arrays.toString(sums));
      for (int i = 0; i < columns.length; i++) {
        globalSums[i].addAndGet(sums[i]);
      }
      //      start = System.currentTimeMillis();
      //      long size = ((AndBlockDocIdSet) blockDocIdSet).getSize();
      //      end = System.currentTimeMillis();
      //System.out.println("size from bitmap::" + size + " time:" + (end - start));
      filterOperator.close();
      totalDocsMatched.addAndGet(matchedCount);
      return null;
    }

  }
}
