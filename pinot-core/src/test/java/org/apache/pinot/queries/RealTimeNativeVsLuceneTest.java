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
package org.apache.pinot.queries;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.search.SearcherManager;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.NativeMutableTextIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndex;
import org.apache.pinot.segment.spi.IndexSegment;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RealTimeNativeVsLuceneTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "RealTimeNativeVsLuceneTest");
  private static final String TEXT_COLUMN_NAME = "testColumnName";

  private RealtimeLuceneTextIndex _realtimeLuceneTextIndex;
  private NativeMutableTextIndex _nativeMutableTextIndex;

  private List<String> getTextData() {
    return Arrays.asList("Prince Andrew kept looking with an amused smile from Pierre",
        "vicomte and from the vicomte to their hostess. In the first moment of",
        "Pierre’s outburst Anna Pávlovna, despite her social experience, was",
        "horror-struck. But when she saw that Pierre’s sacrilegious words",
        "had not exasperated the vicomte, and had convinced herself that it was",
        "impossible to stop him, she rallied her forces and joined the vicomte in", "a vigorous attack on the orator",
        "horror-struck. But when she", "she rallied her forces and joined", "outburst Anna Pávlovna",
        "she rallied her forces and", "despite her social experience", "had not exasperated the vicomte",
        " despite her social experience", "impossible to stop him", "despite her social experience");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _realtimeLuceneTextIndex = new RealtimeLuceneTextIndex(TEXT_COLUMN_NAME, INDEX_DIR, "fooBar");
    _nativeMutableTextIndex = new NativeMutableTextIndex(TEXT_COLUMN_NAME);
    List<String> documents = getTextData();

    for (String doc : documents) {
      _realtimeLuceneTextIndex.add(doc);
      _nativeMutableTextIndex.add(doc);
    }

    SearcherManager searcherManager = _realtimeLuceneTextIndex.getSearcherManager();
    try {
      searcherManager.maybeRefresh();
    } catch (Exception e) {
      // we should never be here since the locking semantics between MutableSegmentImpl::destroy()
      // and this code along with volatile state "isSegmentDestroyed" protect against the cases
      // where this thread might attempt to refresh a realtime lucene reader after it has already
      // been closed duing segment destroy.
    }
  }

  @Override
  protected String getFilter() {
    return null;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return null;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return null;
  }

  @Test
  public void testQueries() {
    String nativeQuery = "vico.*";
    String luceneQuery = "vico*";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "vicomte";
    luceneQuery = "vicomte";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "s.*";
    luceneQuery = "s*";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "impossible";
    luceneQuery = "impossible";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "forc.*s";
    luceneQuery = "forc*s";
    testSelectionResults(nativeQuery, luceneQuery);
  }

  private void testSelectionResults(String nativeQuery, String luceneQuery) {
    MutableRoaringBitmap resultset = _realtimeLuceneTextIndex.getDocIds(luceneQuery);
    Assert.assertNotNull(resultset);

    MutableRoaringBitmap resultset2 = _nativeMutableTextIndex.getDocIds(nativeQuery);
    Assert.assertNotNull(resultset2);

    Assert.assertEquals(resultset.getCardinality(), resultset2.getCardinality());

    PeekableIntIterator intIterator = resultset.getIntIterator();

    while (intIterator.hasNext()) {
      int firstDocId = intIterator.next();
      Assert.assertTrue(resultset2.contains(firstDocId));
    }
  }
}
