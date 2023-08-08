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
import java.util.List;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
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
  private final LuceneTextIndexCreator _indexCreator;
  private SearcherManager _searcherManager;
  private final StandardAnalyzer _analyzer = new StandardAnalyzer();
  private final String _column;
  private final String _segmentName;

  /**
   * Created by {@link MutableSegmentImpl}
   * for each column on which text index has been enabled
   * @param column column name
   * @param segmentIndexDir realtime segment consumer dir
   * @param segmentName realtime segment name
   * @param stopWordsInclude the words to include in addition to the default stop word list
   * @param stopWordsExclude stop words to exclude from default stop words
   */
  public RealtimeLuceneTextIndex(String column, File segmentIndexDir, String segmentName,
      List<String> stopWordsInclude, List<String> stopWordsExclude) {
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
              false /* commitOnClose */, stopWordsInclude, stopWordsExclude);
      IndexWriter indexWriter = _indexCreator.getIndexWriter();
      _searcherManager = new SearcherManager(indexWriter, false, false, null);
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
    Collector docIDCollector = new RealtimeLuceneDocIdCollector(docIDs);
    IndexSearcher indexSearcher = null;
    try {
      Query query = new QueryParser(_column, _analyzer).parse(searchQuery);
      indexSearcher = _searcherManager.acquire();
      indexSearcher.search(query, docIDCollector);
      return getPinotDocIds(indexSearcher, docIDs);
    } catch (Exception e) {
      LOGGER
          .error("Failed while searching the realtime text index for column {}, search query {}, exception {}", _column,
              searchQuery, e.getMessage());
      throw new RuntimeException(e);
    } finally {
      try {
        if (indexSearcher != null) {
          _searcherManager.release(indexSearcher);
        }
      } catch (Exception e) {
        LOGGER.error("Failed while releasing the searcher manager for realtime text index for column {}, exception {}",
            _column, e.getMessage());
      }
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
