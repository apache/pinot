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
package org.apache.pinot.core.realtime.impl.invertedindex;

import java.io.File;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.pinot.core.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.core.segment.index.readers.LuceneTextIndexReader;
import org.apache.pinot.core.segment.index.readers.TextIndexReader;

public class RealtimeLuceneIndexReader implements TextIndexReader<LuceneTextIndexReader.LuceneSearchResult> {
  private final QueryParser _queryParser;
  private final LuceneTextIndexCreator _indexCreator;
  private volatile SearcherManager _searcherManager;

  /**
   * Created by {@link org.apache.pinot.core.indexsegment.mutable.MutableSegmentImpl}
   * for each column on which text index has been enabled
   * @param column column name
   * @param segmentIndexDir realtime segment consumer dir
   * @param segmentName realtime segment name
   */
  public RealtimeLuceneIndexReader(String column, File segmentIndexDir, String segmentName) {
    try {
      _indexCreator = new LuceneTextIndexCreator(column, new File(segmentIndexDir.getAbsolutePath() + "/" + segmentName));
      IndexWriter indexWriter = _indexCreator.getIndexWriter();
      _searcherManager = new SearcherManager(indexWriter, false, false, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate realtime Lucene index reader. Error: " + e);
    }
    StandardAnalyzer analyzer = new StandardAnalyzer();
    _queryParser = new QueryParser(column, analyzer);
  }

  @Override
  public LuceneTextIndexReader.LuceneSearchResult search(String searchQuery) {
    try {
      Query query = _queryParser.parse(searchQuery);
      ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(query);
      IndexSearcher indexSearcher = _searcherManager.acquire();
      TopDocs topDocs = indexSearcher.search(constantScoreQuery, Integer.MAX_VALUE);
      return new LuceneTextIndexReader.LuceneSearchResult(topDocs.scoreDocs, indexSearcher);
    } catch (Exception e) {
      throw new RuntimeException("Error: failed to parse search query: " + searchQuery + " " + e);
    }
  }

  @Override
  public void release(IndexSearcher indexSearcher) {
    try {
      _searcherManager.release(indexSearcher);
    } catch (Exception e) {
      throw new RuntimeException("Error: failed while releasing a previously acquired searcher. " + e);
    }
  }

  public void addDoc(Object doc, int docIdCounter) {
    _indexCreator.addDoc(doc, docIdCounter);
  }

  @Override
  public void close() {
    try {
      _searcherManager.close();
      // TODO: closing this is essentially committing all the lucene index data
      // added during realtime consumption. If we are in this method, it means
      // we are destroying the realtime segment (we would have already committed
      // and converted it into ImmutableSegment). indexCreator.close() is
      // necessary for cleaning up the resources associated with lucene
      // index writer that was indexing data realtime. However,
      // committing it is not necessary since the realtime segment conversion
      // would have already happened and rebuilt the lucene index as part
      // building ImmutableSegment in SegmentColumnarIndexCreator.
      // may be we should just move the lucene index directory for segment conversion
      // so as not pay the cost of building lucene index twice.
      // RealtimeSegmentConverter and SegmentColumnarIndexCreator will have to
      // account for this.
      _indexCreator.close();
    } catch (Exception e) {
      throw new RuntimeException("Error: failed to close SearcherManager. " + e);
    }
  }

  public SearcherManager getSearcherManager() {
    return _searcherManager;
  }
}
