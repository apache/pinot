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
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.utils.LuceneTextIndexUtils;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.mutable.MutableTextIndex;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.LoggerFactory;


/**
 * Lucene text index reader supporting near realtime search. An instance of this
 * is created per consuming segment by {@link MutableSegmentImpl}.
 * Internally it uses {@link LuceneTextIndexCreator} for adding documents to the lucene index
 * as and when they are indexed by the consuming segment.
 */
public class RealtimeLuceneTextIndex implements MutableTextIndex {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(RealtimeLuceneTextIndex.class);
  private static final RealtimeLuceneTextIndexSearcherPool SEARCHER_POOL =
      RealtimeLuceneTextIndexSearcherPool.getInstance();
  private final LuceneTextIndexCreator _indexCreator;
  private SearcherManager _searcherManager;
  private Analyzer _analyzer;
  private final String _column;
  private final String _segmentName;
  private boolean _enablePrefixSuffixMatchingInPhraseQueries = false;

  /**
   * Created by {@link MutableSegmentImpl}
   * for each column on which text index has been enabled
   * @param column column name
   * @param segmentIndexDir realtime segment consumer dir
   * @param segmentName realtime segment name
   * @param config the table index config
   */
  public RealtimeLuceneTextIndex(String column, File segmentIndexDir, String segmentName, TextIndexConfig config) {
    _column = column;
    _segmentName = segmentName;
    try {
      // indexCreator.close() is necessary for cleaning up the resources associated with lucene
      // index writer that was indexing data realtime. We close the indexCreator
      // when the realtime segment is destroyed (we would have already committed the
      // segment and converted it into offline before destroy is invoked)
      // So committing the lucene index for the realtime in-memory segment is not necessary
      // as it is already part of the offline segment after the conversion.
      // This is why "commitOnClose" is set to false when creating the lucene index writer
      // for realtime
      _indexCreator =
          new LuceneTextIndexCreator(column, new File(segmentIndexDir.getAbsolutePath() + "/" + segmentName),
              false /* commitOnClose */, config);
      IndexWriter indexWriter = _indexCreator.getIndexWriter();
      _searcherManager = new SearcherManager(indexWriter, false, false, null);
      _analyzer = _indexCreator.getIndexWriter().getConfig().getAnalyzer();
      _enablePrefixSuffixMatchingInPhraseQueries = config.isEnablePrefixSuffixMatchingInPhraseQueries();
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate realtime Lucene index reader for column {}, exception {}", column,
          e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Adds a new document.
   */
  @Override
  public void add(String document) {
    _indexCreator.add(document);
  }

  /**
   * Adds a new document, made up of multiple values.
   */
  @Override
  public void add(String[] documents) {
    _indexCreator.add(documents, documents.length);
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    MutableRoaringBitmap docIDs = new MutableRoaringBitmap();
    RealtimeLuceneDocIdCollector docIDCollector = new RealtimeLuceneDocIdCollector(docIDs);
    // A thread interrupt during indexSearcher.search() can break the underlying FSDirectory used by the IndexWriter
    // which the SearcherManager is created with. To ensure the index is never corrupted the search is executed
    // in a child thread and the interrupt is handled in the current thread by canceling the search gracefully.
    // See https://github.com/apache/lucene/issues/3315 and https://github.com/apache/lucene/issues/9309
    Callable<MutableRoaringBitmap> searchCallable = () -> {
      IndexSearcher indexSearcher = null;
      try {
        QueryParser parser = new QueryParser(_column, _analyzer);
        parser.setAllowLeadingWildcard(true);
        Query query = parser.parse(searchQuery);
        if (_enablePrefixSuffixMatchingInPhraseQueries) {
          query = LuceneTextIndexUtils.convertToMultiTermSpanQuery(query);
        }
        indexSearcher = _searcherManager.acquire();
        indexSearcher.search(query, docIDCollector);
        return getPinotDocIds(indexSearcher, docIDs);
      } finally {
        try {
          if (indexSearcher != null) {
            _searcherManager.release(indexSearcher);
          }
        } catch (Exception e) {
          LOGGER.error(
              "Failed while releasing the searcher manager for realtime text index for column {}, exception {}",
              _column, e.getMessage());
        }
      }
    };
    Future<MutableRoaringBitmap> searchFuture =
        SEARCHER_POOL.getExecutorService().submit(searchCallable);
    try {
      return searchFuture.get();
    } catch (InterruptedException e) {
      docIDCollector.markShouldCancel();
      LOGGER.warn("TEXT_MATCH query timeout on realtime consuming segment {}, column {}, search query {}", _segmentName,
          _column, searchQuery);
      throw new RuntimeException("TEXT_MATCH query timeout on realtime consuming segment");
    } catch (Exception e) {
      LOGGER.error("Failed while searching the realtime text index for segment {}, column {}, search query {},"
              + " exception {}", _segmentName, _column, searchQuery, e.getMessage());
      throw new RuntimeException(e);
    }
  }

  // TODO: Optimize this similar to how we have done for offline/completed segments.
  // Pre-built mapping will not work for realtime. We need to build an on-the-fly cache
  // as queries are coming in.
  private MutableRoaringBitmap getPinotDocIds(IndexSearcher indexSearcher, MutableRoaringBitmap luceneDocIds) {
    IntIterator luceneDocIDIterator = luceneDocIds.getIntIterator();
    MutableRoaringBitmap actualDocIDs = new MutableRoaringBitmap();
    try {
      while (luceneDocIDIterator.hasNext()) {
        int luceneDocId = luceneDocIDIterator.next();
        Document document = indexSearcher.doc(luceneDocId);
        int pinotDocId = Integer.valueOf(document.get(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME));
        actualDocIDs.add(pinotDocId);
      }
    } catch (Exception e) {
      LOGGER.error("Failure while retrieving document from index for column {}, exception {}", _column, e.getMessage());
      throw new RuntimeException(e);
    }
    return actualDocIDs;
  }

  @Override
  public void close() {
    try {
      _searcherManager.close();
      _searcherManager = null;
      _indexCreator.close();
    } catch (Exception e) {
      LOGGER.error("Failed while closing the realtime text index for column {}, exception {}", _column, e.getMessage());
      throw new RuntimeException(e);
    }
  }

  public SearcherManager getSearcherManager() {
    return _searcherManager;
  }
}
