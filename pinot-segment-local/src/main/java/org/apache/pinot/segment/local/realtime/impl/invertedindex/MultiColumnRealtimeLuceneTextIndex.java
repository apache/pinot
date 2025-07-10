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

import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.text.MultiColumnLuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.local.utils.LuceneTextIndexUtils;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextIndexConstants;
import org.apache.pinot.segment.spi.index.reader.MultiColumnTextIndexReader;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.LoggerFactory;


/**
 * Lucene text index reader supporting near realtime search. An instance of this
 * is created per consuming segment by {@link MutableSegmentImpl}.
 * Internally it uses {@link LuceneTextIndexCreator} for adding documents to the lucene index
 * as and when they are indexed by the consuming segment.
 *
 * A version of RealtimeLuceneTextIndex adapted to work with multiple columns.
 */
public class MultiColumnRealtimeLuceneTextIndex implements MultiColumnTextIndexReader {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MultiColumnRealtimeLuceneTextIndex.class);
  private static final RealtimeLuceneTextIndexSearcherPool SEARCHER_POOL =
      RealtimeLuceneTextIndexSearcherPool.getInstance();
  private final MultiColumnLuceneTextIndexCreator _indexCreator;
  private SearcherManager _searcherManager;
  private Analyzer _analyzer;
  private Constructor<QueryParserBase> _queryParserClassConstructor;

  private final List<String> _columns;
  private final String _segmentName;
  private final boolean _reuseMutableIndex;
  private boolean _enablePrefixSuffixMatchingInPhraseQueries = false;
  private final RealtimeLuceneRefreshListener _refreshListener;
  private final RealtimeLuceneIndexRefreshManager.SearcherManagerHolder _searcherManagerHolder;
  private final Map<String, MultiColumnLuceneTextIndexReader.ColumnConfig> _perColumnConfigs;

  /**
   * Created by {@link MutableSegmentImpl}
   * for each column on which text index has been enabled
   * @param columns column names
   * @param segmentIndexDir realtime segment consumer dir
   * @param segmentName realtime segment name
   * @param mcTextConfig the table index config
   */
  public MultiColumnRealtimeLuceneTextIndex(
      List<String> columns,
      BooleanList columnsSV,
      File segmentIndexDir,
      String segmentName,
      MultiColumnTextIndexConfig mcTextConfig) {
    _columns = columns;
    _segmentName = segmentName;
    try {
      _perColumnConfigs =
          MultiColumnLuceneTextIndexReader.buildColumnConfigs(mcTextConfig.getPerColumnProperties());

      TextIndexConfig config = new TextIndexConfigBuilder().withProperties(mcTextConfig.getProperties()).build();
      // indexCreator.close() is necessary for cleaning up the resources associated with lucene
      // index writer that was indexing data realtime. We close the indexCreator
      // when the realtime segment is destroyed (we would have already committed the
      // segment and converted it into offline before destroy is invoked)
      // So committing the lucene index for the realtime in-memory segment is not necessary
      // as it is already part of the offline segment after the conversion.
      // This is why "commitOnClose" is set to false when creating the lucene index writer
      // for realtime
      _indexCreator =
          new MultiColumnLuceneTextIndexCreator(columns, columnsSV,
              new File(segmentIndexDir.getAbsolutePath() + "/" + segmentName),
              false /* commitOnClose */, false, null, null, mcTextConfig);
      IndexWriter indexWriter = _indexCreator.getIndexWriter();
      _searcherManager = new SearcherManager(indexWriter, false, false, null);

      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      _refreshListener = new RealtimeLuceneRefreshListener(llcSegmentName.getTableName(), segmentName,
          MultiColumnTextIndexConstants.INDEX_DIR_NAME,
          llcSegmentName.getPartitionGroupId(), _indexCreator::getNumDocs);
      _searcherManager.addListener(_refreshListener);
      _analyzer = _indexCreator.getIndexWriter().getConfig().getAnalyzer();
      _queryParserClassConstructor =
          TextIndexUtils.getQueryParserWithStringAndAnalyzerTypeConstructor(config.getLuceneQueryParserClass());
      _enablePrefixSuffixMatchingInPhraseQueries = config.isEnablePrefixSuffixMatchingInPhraseQueries();
      _reuseMutableIndex = config.isReuseMutableIndex();

      // Submit the searcher manager to the global pool for refreshing
      _searcherManagerHolder =
          new RealtimeLuceneIndexRefreshManager.SearcherManagerHolder(segmentName,
              MultiColumnTextIndexConstants.INDEX_DIR_NAME, _searcherManager);
      RealtimeLuceneIndexRefreshManager.getInstance().addSearcherManagerHolder(_searcherManagerHolder);
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate realtime Lucene index reader for columns {}, exception {}", columns,
          e.getMessage());
      throw new RuntimeException(e);
    }
  }

  public void add(List<Object> values) {
    _indexCreator.add(values);
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    throw new UnsupportedOperationException("Multi-column text index requires column name to query!");
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    throw new UnsupportedOperationException("Multi-column text index requires column name to query!");
  }

  @Override
  public MutableRoaringBitmap getDocIds(String column, String searchQuery, @Nullable String optionsString) {
    if (optionsString != null && !optionsString.trim().isEmpty()) {
      LuceneTextIndexUtils.LuceneTextIndexOptions options = LuceneTextIndexUtils.createOptions(optionsString);
      Map<String, String> optionsMap = options.getOptions();
      if (!optionsMap.isEmpty()) {
        return getDocIdsWithOptions(column, searchQuery, options);
      }
    }
    return getDocIdsWithoutOptions(column, searchQuery);
  }

  @Override
  public MutableRoaringBitmap getDocIds(String column, String searchQuery) {
    return getDocIdsWithoutOptions(column, searchQuery);
  }

  // TODO: Consider creating a base class (e.g., BaseLuceneTextIndexReader) to avoid code duplication
  // for getDocIdsWithOptions method across LuceneTextIndexReader, MultiColumnLuceneTextIndexReader,
  // RealtimeLuceneTextIndex, and MultiColumnRealtimeLuceneTextIndex
  private MutableRoaringBitmap getDocIdsWithOptions(String column, String actualQuery,
      LuceneTextIndexUtils.LuceneTextIndexOptions options) {
    MutableRoaringBitmap docIDs = new MutableRoaringBitmap();
    RealtimeLuceneDocIdCollector docIDCollector = new RealtimeLuceneDocIdCollector(docIDs);
    // A thread interrupt during indexSearcher.search() can break the underlying FSDirectory used by the IndexWriter
    // which the SearcherManager is created with. To ensure the index is never corrupted the search is executed
    // in a child thread and the interrupt is handled in the current thread by canceling the search gracefully.
    // See https://github.com/apache/lucene/issues/3315 and https://github.com/apache/lucene/issues/9309
    Callable<MutableRoaringBitmap> searchCallable = () -> {
      IndexSearcher indexSearcher = null;
      try {
        Query query = LuceneTextIndexUtils.createQueryParserWithOptions(actualQuery, options, column, _analyzer);
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
              "Failed while releasing the searcher manager for realtime text index for columns {}, exception {}",
              _columns, e.getMessage());
        }
      }
    };
    Future<MutableRoaringBitmap> searchFuture = SEARCHER_POOL.getExecutorService().submit(searchCallable);
    try {
      return searchFuture.get();
    } catch (InterruptedException e) {
      docIDCollector.markShouldCancel();
      throw new RuntimeException("TEXT_MATCH query interrupted while querying the consuming segment " + _segmentName
          + " for columns " + _columns + " with search query: " + actualQuery, e);
    } catch (Exception e) {
      throw new RuntimeException("Failed while searching the realtime text index for segment " + _segmentName
          + " for columns " + _columns + " with search query: " + actualQuery, e);
    }
  }

  private MutableRoaringBitmap getDocIdsWithoutOptions(String column, String searchQuery) {
    MutableRoaringBitmap docIDs = new MutableRoaringBitmap();
    RealtimeLuceneDocIdCollector docIDCollector = new RealtimeLuceneDocIdCollector(docIDs);
    // A thread interrupt during indexSearcher.search() can break the underlying FSDirectory used by the IndexWriter
    // which the SearcherManager is created with. To ensure the index is never corrupted the search is executed
    // in a child thread and the interrupt is handled in the current thread by canceling the search gracefully.
    // See https://github.com/apache/lucene/issues/3315 and https://github.com/apache/lucene/issues/9309
    Callable<MutableRoaringBitmap> searchCallable = () -> {
      IndexSearcher indexSearcher = null;
      try {

        // Lucene query parsers are generally stateful and a new instance must be created per query.
        Constructor<QueryParserBase> queryParserClassConstructor = _queryParserClassConstructor;
        boolean enablePrefixSuffixMatchingInPhraseQueries = _enablePrefixSuffixMatchingInPhraseQueries;
        MultiColumnLuceneTextIndexReader.ColumnConfig columnConfig = _perColumnConfigs.get(column);
        if (columnConfig != null) {
          if (columnConfig.getQueryParserClassConstructor() != null) {
            queryParserClassConstructor = columnConfig.getQueryParserClassConstructor();
          }
          if (columnConfig.getEnablePrefixSuffixMatchingInPhraseQueries() != null) {
            enablePrefixSuffixMatchingInPhraseQueries = columnConfig.getEnablePrefixSuffixMatchingInPhraseQueries();
          }
        }

        QueryParserBase parser = queryParserClassConstructor.newInstance(column, _analyzer);
        if (enablePrefixSuffixMatchingInPhraseQueries) {
          // Note: Lucene's built-in QueryParser has limited wildcard functionality in phrase queries. It does not use
          // the provided analyzer when wildcards are present, defaulting to the default analyzer for tokenization.
          // Additionally, it does not support wildcards that span across terms.
          // For more details, see: https://github.com/elastic/elasticsearch/issues/22540
          // Workaround: Use a custom query parser that correctly implements wildcard searches.
          parser.setAllowLeadingWildcard(true);
        }
        Query query = parser.parse(searchQuery);
        if (enablePrefixSuffixMatchingInPhraseQueries) {
          // Note: Lucene's built-in QueryParser has limited wildcard functionality in phrase queries. It does not use
          // the provided analyzer when wildcards are present, defaulting to the default analyzer for tokenization.
          // Additionally, it does not support wildcards that span across terms.
          // For more details, see: https://github.com/elastic/elasticsearch/issues/22540
          // Workaround: Use a custom query parser that correctly implements wildcard searches.
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
              "Failed while releasing the searcher manager for realtime text index for columns {}, exception {}",
              _columns, e.getMessage());
        }
      }
    };
    Future<MutableRoaringBitmap> searchFuture = SEARCHER_POOL.getExecutorService().submit(searchCallable);
    try {
      return searchFuture.get();
    } catch (InterruptedException e) {
      docIDCollector.markShouldCancel();
      throw new RuntimeException("TEXT_MATCH query interrupted while querying the consuming segment " + _segmentName
          + " for columns " + _columns + " with search query: " + searchQuery, e);
    } catch (Exception e) {
      throw new RuntimeException("Failed while searching the realtime text index for segment " + _segmentName
          + " for columns " + _columns + " with search query: " + searchQuery, e);
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
        int pinotDocId = Integer.parseInt(document.get(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME));
        actualDocIDs.add(pinotDocId);
      }
    } catch (Exception e) {
      LOGGER.error("Failure while retrieving document from index for columns {}, exception {}", _columns,
          e.getMessage());
      throw new RuntimeException(e);
    }
    return actualDocIDs;
  }

  private Constructor<QueryParserBase> getQueryParserWithStringAndAnalyzerTypeConstructor(String queryParserClassName)
      throws ReflectiveOperationException {
    // Fail-fast if the query parser is specified class is not QueryParseBase class
    final Class<?> queryParserClass = Class.forName(queryParserClassName);
    if (!QueryParserBase.class.isAssignableFrom(queryParserClass)) {
      throw new ReflectiveOperationException("The specified lucene query parser class " + queryParserClassName
          + " is not assignable from " + QueryParserBase.class.getName());
    }
    // Fail-fast if the query parser does not have the required constructor used by this class
    try {
      queryParserClass.getConstructor(String.class, Analyzer.class);
    } catch (NoSuchMethodException ex) {
      throw new NoSuchMethodException("The specified lucene query parser class " + queryParserClassName
          + " is not assignable because the class does not have the required constructor method with parameter "
          + "type [String.class, Analyzer.class]"
      );
    }

    return (Constructor<QueryParserBase>) queryParserClass.getConstructor(String.class, Analyzer.class);
  }

  public void commit() {
    if (!_reuseMutableIndex) {
      return;
    }
    try {
      _indexCreator.getIndexWriter().commit();
      // Set the SearcherManagerHolder.indexClosed() flag to stop generating refreshed readers
      _searcherManagerHolder.getLock().lock();
      try {
        _searcherManagerHolder.setIndexClosed();
        // Block for one final refresh, to ensure queries are fully up to date while segment is being converted
        _searcherManager.maybeRefreshBlocking();
      } finally {
        _searcherManagerHolder.getLock().unlock();
      }
      // It is OK to close the index writer as we are done indexing, and no more refreshes will take place
      // The SearcherManager will still provide an up-to-date reader via .acquire()
      _indexCreator.getIndexWriter().close();
    } catch (Exception e) {
      LOGGER.error("Failed to commit the realtime lucene text index for columns {}, exception {}", _columns,
          e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      // Set the SearcherManagerHolder.indexClosed() flag to stop generating refreshed readers. If completionMode is
      // set as DOWNLOAD, then commit() will not be called the flag must be set here.
      _searcherManagerHolder.getLock().lock();
      try {
        _searcherManagerHolder.setIndexClosed();
      } finally {
        _searcherManagerHolder.getLock().unlock();
      }
      _searcherManager.close();
      _searcherManager = null;
      _refreshListener.close(); // clean up metrics prior to closing _indexCreator, as they contain a reference to it
      _indexCreator.close();
      _analyzer.close();
    } catch (Exception e) {
      LOGGER.error("Failed while closing the realtime text index for columns {}, exception {}", _columns,
          e.getMessage());
      throw new RuntimeException(e);
    }
  }

  public SearcherManager getSearcherManager() {
    return _searcherManager;
  }

  public Object2IntOpenHashMap getMapping() {
    Object2IntOpenHashMap<Object> mapping = new Object2IntOpenHashMap<>();
    mapping.defaultReturnValue(-1);

    for (int i = 0; i < _columns.size(); i++) {
      mapping.put(_columns.get(i), i);
    }

    return mapping;
  }

  @Override
  public boolean isMultiColumn() {
    return true;
  }
}
