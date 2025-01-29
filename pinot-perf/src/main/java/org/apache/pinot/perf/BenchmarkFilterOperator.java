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
package org.apache.pinot.perf;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.utils.ReadMode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 50)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkFilterOperator {
  private static final Random RANDOM = new Random();
  private static final int MAX_DOC_PER_CALL = DocIdSetPlanNode.MAX_DOC_PER_CALL;
  private static final String TABLE_NAME = "tbl1";

  private ImmutableSegment _immutableSegment;
  private FilterContext _filterContext;

  @Setup
  public void setUp()
      throws Exception {
    // Load segment
    String segmentDirStr = System.getenv("PERF_TEST_SEGMENT_DIR");
    File segmentDir = new File(segmentDirStr);
    _immutableSegment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);
    // Load filter
    String filterExprFile = System.getenv("PERF_TEST_FILTER_EXP_FILE");
    String filter = FileUtils.readFileToString(new File(filterExprFile), StandardCharsets.UTF_8);
    _filterContext = RequestContextUtils.getFilter(RequestContextUtils.getExpression(filter));
  }

  @TearDown
  public void tearDown()
      throws Exception {
  }

  @Benchmark
  public int testFilterDocIdSetOperator() {
    SegmentContext segmentContext = new SegmentContext(_immutableSegment);
    QueryContext queryContext = buildQueryContext(_filterContext);
    FilterPlanNode filterPlanNode = new FilterPlanNode(segmentContext, queryContext, _filterContext);
    DocIdSetPlanNode docIdSetPlanNode = new DocIdSetPlanNode(segmentContext, queryContext, MAX_DOC_PER_CALL,
        filterPlanNode.run());
    DocIdSetOperator docIdSetOperator = docIdSetPlanNode.run();
    DocIdSetBlock block = null;
    int result = 0;
    while ((block = docIdSetOperator.nextBlock()) != null) {
      result += block.getLength();
    }
    return result;
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkFilterOperator.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  private QueryContext buildQueryContext(FilterContext filterContext) {
    return new QueryContext.Builder().setFilter(filterContext).setTableName(TABLE_NAME).setLimit(Integer.MAX_VALUE)
        .setAliasList(Collections.emptyList()).setGroupByExpressions(Collections.emptyList())
        .setSelectExpressions(Collections.emptyList()).build();
  }
}
