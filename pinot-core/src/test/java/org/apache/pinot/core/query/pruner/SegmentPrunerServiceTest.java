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

package org.apache.pinot.core.query.pruner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.pinot.core.query.config.SegmentPrunerConfig;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentPrunerServiceTest {
  private final SegmentPrunerConfig _emptyPrunerConf;

  public SegmentPrunerServiceTest() {
    PinotConfiguration pinotConf = new PinotConfiguration();
    pinotConf.setProperty(Server.CLASS, "[]");
    _emptyPrunerConf = new SegmentPrunerConfig(pinotConf);
  }

  @Test
  public void notEmptyValidSegmentsAreNotPruned() {
    SegmentPrunerService service = new SegmentPrunerService(_emptyPrunerConf);
    IndexSegment indexSegment = mockIndexSegment(10, "col1", "col2");

    SegmentPrunerStatistics stats = new SegmentPrunerStatistics();

    List<IndexSegment> indexes = new ArrayList<>();
    indexes.add(indexSegment);

    String query = "select col1 from t1";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    List<IndexSegment> actual = service.prune(indexes, queryContext, stats);

    Assert.assertEquals(actual, indexes);
    Assert.assertEquals(stats.getInvalidSegments(), 0);
  }

  @Test
  public void emptySegmentsAreNotInvalid() {
    SegmentPrunerService service = new SegmentPrunerService(_emptyPrunerConf);
    IndexSegment indexSegment = mockIndexSegment(0, "col1", "col2");

    SegmentPrunerStatistics stats = new SegmentPrunerStatistics();

    List<IndexSegment> indexes = new ArrayList<>();
    indexes.add(indexSegment);

    String query = "select col1 from t1";

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    List<IndexSegment> actual = service.prune(indexes, queryContext, stats);

    Assert.assertEquals(actual, Collections.emptyList());
    Assert.assertEquals(stats.getInvalidSegments(), 0);
  }

  @Test
  public void emptyValidPruned() {
    SegmentPrunerService service = new SegmentPrunerService(_emptyPrunerConf);
    IndexSegment segment = mockUpsertIndexSegment(10, new ThreadSafeMutableRoaringBitmap(), null);

    List<IndexSegment> segments = new ArrayList<>();
    segments.add(segment);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("select col1 from t1");

    List<IndexSegment> actual = service.prune(segments, queryContext, new SegmentPrunerStatistics());

    Assert.assertEquals(actual, Collections.emptyList());
  }

  @Test
  public void emptyValidRetainedWithSkipUpsert() {
    SegmentPrunerService service = new SegmentPrunerService(_emptyPrunerConf);
    IndexSegment segment = mockUpsertIndexSegment(10, new ThreadSafeMutableRoaringBitmap(), null);

    List<IndexSegment> segments = new ArrayList<>();
    segments.add(segment);
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("select col1 from t1 option(skipUpsert=true)");

    List<IndexSegment> actual = service.prune(segments, queryContext, new SegmentPrunerStatistics());

    Assert.assertEquals(actual, segments);
  }

  @Test
  public void nonEmptyValidNotPrunedWhenQueryableNull() {
    SegmentPrunerService service = new SegmentPrunerService(_emptyPrunerConf);
    IndexSegment segment = mockUpsertIndexSegment(10, new ThreadSafeMutableRoaringBitmap(0), null);

    List<IndexSegment> indexes = new ArrayList<>();
    indexes.add(segment);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("select col1 from t1");

    List<IndexSegment> actual = service.prune(indexes, queryContext, new SegmentPrunerStatistics());

    Assert.assertEquals(actual, indexes);
  }

  @Test
  public void nonUpsertSegmentWithNullDocIdBitmapsNotPruned() {
    SegmentPrunerService service = new SegmentPrunerService(_emptyPrunerConf);
    IndexSegment segment = mockUpsertIndexSegment(5, null, null);

    List<IndexSegment> segments = new ArrayList<>();
    segments.add(segment);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("select col1 from t1");

    List<IndexSegment> actual = service.prune(segments, queryContext, new SegmentPrunerStatistics());

    Assert.assertEquals(actual, segments);
  }

  /**
   * When queryable doc ids exist, a segment with no queryable rows is pruned even if valid doc ids still hold
   * replaced rows (matches normal upsert query semantics).
   */
  @Test
  public void emptyQueryablePruned() {
    SegmentPrunerService service = new SegmentPrunerService(_emptyPrunerConf);
    ThreadSafeMutableRoaringBitmap valid = new ThreadSafeMutableRoaringBitmap(0);
    ThreadSafeMutableRoaringBitmap queryable = new ThreadSafeMutableRoaringBitmap();
    IndexSegment segment = mockUpsertIndexSegment(10, valid, queryable);

    List<IndexSegment> segments = new ArrayList<>();
    segments.add(segment);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("select col1 from t1");

    List<IndexSegment> actual = service.prune(segments, queryContext, new SegmentPrunerStatistics());

    Assert.assertEquals(actual, Collections.emptyList());
  }

  @Test
  public void emptyQueryableRetainedWithSkipUpsert() {
    SegmentPrunerService service = new SegmentPrunerService(_emptyPrunerConf);
    ThreadSafeMutableRoaringBitmap valid = new ThreadSafeMutableRoaringBitmap(0);
    ThreadSafeMutableRoaringBitmap queryable = new ThreadSafeMutableRoaringBitmap();
    IndexSegment segment = mockUpsertIndexSegment(10, valid, queryable);

    List<IndexSegment> segments = new ArrayList<>();
    segments.add(segment);
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("select col1 from t1 option(skipUpsert=true)");

    List<IndexSegment> actual = service.prune(segments, queryContext, new SegmentPrunerStatistics());

    Assert.assertEquals(actual, segments);
  }

  @Test
  public void nonEmptyQueryableNotPruned() {
    SegmentPrunerService service = new SegmentPrunerService(_emptyPrunerConf);
    ThreadSafeMutableRoaringBitmap valid = new ThreadSafeMutableRoaringBitmap(0);
    ThreadSafeMutableRoaringBitmap queryable = new ThreadSafeMutableRoaringBitmap(0);
    IndexSegment segment = mockUpsertIndexSegment(10, valid, queryable);

    List<IndexSegment> indexes = new ArrayList<>();
    indexes.add(segment);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("select col1 from t1");

    List<IndexSegment> actual = service.prune(indexes, queryContext, new SegmentPrunerStatistics());

    Assert.assertEquals(actual, indexes);
  }

  /**
   * Queryable bitmap takes precedence over valid when both are present.
   */
  @Test
  public void nonEmptyQueryableOverridesEmptyValid() {
    SegmentPrunerService service = new SegmentPrunerService(_emptyPrunerConf);
    ThreadSafeMutableRoaringBitmap valid = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap queryable = new ThreadSafeMutableRoaringBitmap(0);
    IndexSegment segment = mockUpsertIndexSegment(10, valid, queryable);

    List<IndexSegment> indexes = new ArrayList<>();
    indexes.add(segment);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("select col1 from t1");

    List<IndexSegment> actual = service.prune(indexes, queryContext, new SegmentPrunerStatistics());

    Assert.assertEquals(actual, indexes);
  }

  private IndexSegment mockIndexSegment(int totalDocs, String... columns) {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(new HashSet<>(Arrays.asList(columns)));
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(totalDocs);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    return indexSegment;
  }

  /**
   * Segment with upsert-style doc id bitmaps (valid and optional queryable).
   */
  private IndexSegment mockUpsertIndexSegment(int totalDocs,
      ThreadSafeMutableRoaringBitmap validDocIds, ThreadSafeMutableRoaringBitmap queryableDocIds) {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(new HashSet<>(Arrays.asList("col1")));
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(totalDocs);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(indexSegment.getValidDocIds()).thenReturn(validDocIds);
    when(indexSegment.getQueryableDocIds()).thenReturn(queryableDocIds);
    return indexSegment;
  }
}
