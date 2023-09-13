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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.search.SearcherManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class NativeAndLuceneMutableTextIndexTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "RealTimeNativeVsLuceneTest");
  private static final String TEXT_COLUMN_NAME = "testColumnName";
  private static final String MV_TEXT_COLUMN_NAME = "testMVColumnName";

  private RealtimeLuceneTextIndex _realtimeLuceneTextIndex;
  private NativeMutableTextIndex _nativeMutableTextIndex;

  private RealtimeLuceneTextIndex _realtimeLuceneMVTextIndex;
  private NativeMutableTextIndex _nativeMutableMVTextIndex;

  private String[] getTextData() {
    return new String[]{"Prince Andrew kept looking with an amused smile from Pierre",
      "vicomte and from the vicomte to their hostess. In the first moment of",
      "Pierre’s outburst Anna Pávlovna, despite her social experience, was",
      "horror-struck. But when she saw that Pierre’s sacrilegious words",
      "had not exasperated the vicomte, and had convinced herself that it was",
      "impossible to stop him, she rallied her forces and joined the vicomte in", "a vigorous attack on the orator",
      "horror-struck. But when she", "she rallied her forces and joined", "outburst Anna Pávlovna",
      "she rallied her forces and", "despite her social experience", "had not exasperated the vicomte",
      " despite her social experience", "impossible to stop him", "despite her social experience"};
  }

  private String[][] getMVTextData() {
    return new String[][]{{"Prince Andrew kept looking with an amused smile from Pierre",
        "vicomte and from the vicomte to their hostess. In the first moment of"}, {
      "Pierre’s outburst Anna Pávlovna, despite her social experience, was",
        "horror-struck. But when she saw that Pierre’s sacrilegious words"}, {
      "had not exasperated the vicomte, and had convinced herself that it was"}, {
      "impossible to stop him, she rallied her forces and joined the vicomte in", "a vigorous attack on the orator",
        "horror-struck. But when she", "she rallied her forces and joined", "outburst Anna Pávlovna"}, {
      "she rallied her forces and", "despite her social experience", "had not exasperated the vicomte",
        " despite her social experience", "impossible to stop him", "despite her social experience"}};
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _realtimeLuceneTextIndex =
        new RealtimeLuceneTextIndex(TEXT_COLUMN_NAME, INDEX_DIR, "fooBar", null, null, true, 500);
    _nativeMutableTextIndex = new NativeMutableTextIndex(TEXT_COLUMN_NAME);

    _realtimeLuceneMVTextIndex =
        new RealtimeLuceneTextIndex(MV_TEXT_COLUMN_NAME, INDEX_DIR, "fooBar", null, null, true, 500);
    _nativeMutableMVTextIndex = new NativeMutableTextIndex(MV_TEXT_COLUMN_NAME);

    String[] documents = getTextData();
    for (String doc : documents) {
      _realtimeLuceneTextIndex.add(doc);
      _nativeMutableTextIndex.add(doc);
    }

    String[][] mvDocuments = getMVTextData();
    for (String[] mvDoc : mvDocuments) {
      _realtimeLuceneMVTextIndex.add(mvDoc);
      _nativeMutableMVTextIndex.add(mvDoc);
    }

    List<SearcherManager> searcherManagers = new ArrayList<>();
    searcherManagers.add(_realtimeLuceneTextIndex.getSearcherManager());
    searcherManagers.add(_realtimeLuceneMVTextIndex.getSearcherManager());
    try {
      for (SearcherManager searcherManager : searcherManagers) {
        searcherManager.maybeRefresh();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public void tearDown() {
    _realtimeLuceneTextIndex.close();
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
    assertEquals(_nativeMutableTextIndex.getDocIds(nativeQuery), _realtimeLuceneTextIndex.getDocIds(luceneQuery));
    assertEquals(_nativeMutableMVTextIndex.getDocIds(nativeQuery), _realtimeLuceneMVTextIndex.getDocIds(luceneQuery));
  }
}
