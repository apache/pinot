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
package org.apache.pinot.core.segment.index.readers.text;

import java.io.File;
import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.core.segment.creator.impl.inv.text.LuceneTextIndexCreator;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.store.SegmentDirectoryPaths;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.LoggerFactory;


/**
 * This is used to read/search the Lucene text index.
 * When {@link org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader} loads the segment,
 * it also loads (mmaps) the Lucene text index if the segment has TEXT column(s).
 */
public class LuceneTextIndexReader implements InvertedIndexReader<MutableRoaringBitmap> {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexReader.class);

  private final IndexReader _indexReader;
  private final Directory _indexDirectory;
  private final IndexSearcher _indexSearcher;
  private final QueryParser _queryParser;
  private final String _column;

  /**
   * As part of loading the segment in ImmutableSegmentLoader,
   * we load the text index (per column if it exists) and store
   * the reference in {@link org.apache.pinot.core.segment.index.column.PhysicalColumnIndexContainer}
   * similar to how it is done for other types of indexes.
   * @param column column name
   * @param segmentIndexDir segment index directory
   */
  public LuceneTextIndexReader(String column, File segmentIndexDir) {
    _column = column;
    try {
      File indexFile = getTextIndexFile(segmentIndexDir);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      _indexReader = DirectoryReader.open(_indexDirectory);
      _indexSearcher = new IndexSearcher(_indexReader);
      // Disable Lucene query result cache. While it helps a lot with performance for
      // repeated queries, on the downside it cause heap issues.
      _indexSearcher.setQueryCache(null);
    } catch (Exception e) {
      LOGGER
          .error("Failed to instantiate Lucene text index reader for column {}, exception {}", column, e.getMessage());
      throw new RuntimeException(e);
    }
    StandardAnalyzer analyzer = new StandardAnalyzer();
    _queryParser = new QueryParser(column, analyzer);
  }

  /**
   * CASE 1: If IndexLoadingConfig specifies a segment version to load and if it is different then
   * the on-disk version of the segment, then {@link org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader}
   * will take care of up-converting the on-disk segment to v3 before load. The converter
   * already has support for converting v1 text index to v3. So the text index can be
   * loaded from segmentIndexDir/v3/ since v3 sub-directory would have already been created
   *
   * CASE 2: However, if IndexLoadingConfig doesn't specify the segment version to load or if the specified
   * version is same as the on-disk version of the segment, then ImmutableSegmentLoader will load
   * whatever the version of segment is on disk.
   * @param segmentIndexDir top-level segment index directory
   * @return text index file
   */
  private File getTextIndexFile(File segmentIndexDir) {
    // will return null if file does not exist
    File file = SegmentDirectoryPaths.findTextIndexIndexFile(segmentIndexDir, _column);
    if (file == null) {
      throw new IllegalStateException("Failed to find text index file for column: " + _column);
    }
    return file;
  }

  @Override
  public MutableRoaringBitmap getDocIds(int dictId) {
    // This should not be called from anywhere. If it happens, there is a bug in the current implementation
    // and that's why we throw illegal state exception
    throw new IllegalStateException("Using dictionary ID is not supported on Lucene inverted index");
  }

  /**
   * Get docIds from the text inverted index for a given raw value
   * @param value value to look for in the inverted index
   * @return docIDs in bitmap
   */
  @Override
  public MutableRoaringBitmap getDocIds(Object value) {
    String searchQuery = (String) value;
    MutableRoaringBitmap docIDs = new MutableRoaringBitmap();
    Collector docIDCollector = new LuceneDocIdCollector(docIDs);
    try {
      Query query = _queryParser.parse(searchQuery);
      _indexSearcher.search(query, docIDCollector);
      return getPinotDocIds(docIDs);
    } catch (Exception e) {
      LOGGER.error("Failed while searching the text index for column {}, search query {}, exception {}", _column,
          searchQuery, e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Lucene docIDs are not same as pinot docIDs. The internal implementation
   * of Lucene can change the docIds and they are not guaranteed to be the
   * same as how we expect -- strictly increasing docIDs as the documents
   * are ingested during segment/index creation.
   * Therefore, once the search query in Lucene returns the matching
   * Lucene internal docIds, we can then get the corresponding pinot docID
   * since we had stored them in Lucene as a non-indexable field.
   * @param luceneDocIds matching docIDs returned by lucene search query
   * @return bitmap containing pinot docIDs
   *
   * TODO: Explore optimizing this path to avoid building the second bitmap
   */
  private MutableRoaringBitmap getPinotDocIds(MutableRoaringBitmap luceneDocIds) {
    IntIterator luceneDocIDIterator = luceneDocIds.getIntIterator();
    MutableRoaringBitmap actualDocIDs = new MutableRoaringBitmap();
    try {
      while (luceneDocIDIterator.hasNext()) {
        int luceneDocId = luceneDocIDIterator.next();
        Document document = _indexSearcher.doc(luceneDocId);
        int pinotDocId = Integer.valueOf(document.get(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME));
        actualDocIDs.add(pinotDocId);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error: failed while retrieving document from index: " + e);
    }
    return actualDocIDs;
  }

  /**
   * When we destroy the loaded ImmutableSegment, all the indexes
   * (for each column) are destroyed and as part of that
   * we release the text index
   * @throws IOException
   */
  @Override
  public void close()
      throws IOException {
    _indexReader.close();
    _indexDirectory.close();
  }
}
