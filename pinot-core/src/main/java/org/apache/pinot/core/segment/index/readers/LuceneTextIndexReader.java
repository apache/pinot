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
package org.apache.pinot.core.segment.index.readers;

import java.io.File;
import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.core.segment.creator.impl.text.LuceneTextIndexCreator;

public class LuceneTextIndexReader implements TextIndexReader<LuceneTextIndexReader.LuceneSearchResult> {
  private final IndexReader _indexReader;
  private final Directory _indexDirectory;
  private final IndexSearcher _indexSearcher;
  private final QueryParser _queryParser;

  /**
   * As part of loading the segment in ImmutableSegmentLoader,
   * we load the text index (per column if it exists) and store
   * the reference in {@link org.apache.pinot.core.segment.index.column.PhysicalColumnIndexContainer}
   * similar to how it is done for other types of indexes.
   * @param column column name
   * @param segmentIndexDir segment index directory
   */
  public LuceneTextIndexReader(String column, File segmentIndexDir) {
    try {
      File indexFile = new File(segmentIndexDir.getPath() + "/" + column + LuceneTextIndexCreator.DEFAULT_INDEX_FILE_EXTENSION);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      _indexReader = DirectoryReader.open(_indexDirectory);
      _indexSearcher = new IndexSearcher(_indexReader);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate Lucene text index reader. Error: " + e);
    }
    StandardAnalyzer analyzer = new StandardAnalyzer();
    _queryParser = new QueryParser(column, analyzer);
  }

  /**
   * Called during filter operator execution
   * by {@link org.apache.pinot.core.operator.filter.TextMatchFilterOperator}
   * @param searchQuery text search query string
   * @return search results
   */
  @Override
  public LuceneSearchResult search(String searchQuery) {
    try {
      Query query = _queryParser.parse(searchQuery);
      ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(query);
      TopDocs topDocs = _indexSearcher.search(constantScoreQuery, Integer.MAX_VALUE);
      return new LuceneSearchResult(topDocs.scoreDocs, _indexSearcher);
    } catch (Exception e) {
      throw new RuntimeException("Error: failed to parse search query: " + searchQuery + " " + e);
    }
  }

  /**
   * When we destroy the loaded ImmutableSegment, all the indexes
   * (for each column) are destroyed and as part of that
   * we release the text index
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    _indexReader.close();
    _indexDirectory.close();
  }

  @Override
  public void release(IndexSearcher indexSearcher) {
    // NO-OP
    // this interface exists only because of Lucene realtime reader
    // TODO: fix this
  }

  public static class LuceneSearchResult {
    final ScoreDoc[] _scoreDocs;
    final IndexSearcher _indexSearcher;

    public LuceneSearchResult(ScoreDoc[] scoreDocs, IndexSearcher indexSearcher) {
      _scoreDocs = scoreDocs;
      _indexSearcher = indexSearcher;
    }

    public IndexSearcher getIndexSearcher() {
      return _indexSearcher;
    }

    public ScoreDoc[] getScoreDocs() {
      return _scoreDocs;
    }
  }
}
