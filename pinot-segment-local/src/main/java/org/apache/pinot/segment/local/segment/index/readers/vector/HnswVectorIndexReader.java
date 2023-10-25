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
package org.apache.pinot.segment.local.segment.index.readers.vector;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.LoggerFactory;


public class HnswVectorIndexReader implements VectorIndexReader {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexReader.class);

  private final IndexReader _indexReader;
  private final Directory _indexDirectory;
  private final IndexSearcher _indexSearcher;
  private final String _column;
  private final HnswVectorIndexReader.DocIdTranslator _docIdTranslator;
  private boolean _useANDForMultiTermQueries = false;

  public HnswVectorIndexReader(String column, File indexDir, int numDocs, VectorIndexConfig config) {
    _column = column;
    try {
      File indexFile = getVectorIndexFile(indexDir);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      _indexReader = DirectoryReader.open(_indexDirectory);
      _indexSearcher = new IndexSearcher(_indexReader);

//      if (!config.isEnableQueryCache()) {
//        // Disable Lucene query result cache. While it helps a lot with performance for
//        // repeated queries, on the downside it cause heap issues.
//        _indexSearcher.setQueryCache(null);
//      }
//      if (config.isUseANDForMultiTermQueries()) {
//        _useANDForMultiTermQueries = true;
//      }
      // TODO: consider using a threshold of num docs per segment to decide between building
      // mapping file upfront on segment load v/s on-the-fly during query processing
      _docIdTranslator = new HnswVectorIndexReader.DocIdTranslator(indexDir, _column, numDocs, _indexSearcher);
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate Lucene text index reader for column {}, exception {}", column,
          e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * CASE 1: If IndexLoadingConfig specifies a segment version to load and if it is different then
   * the on-disk version of the segment, then {@link ImmutableSegmentLoader}
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
  private File getVectorIndexFile(File segmentIndexDir) {
    // will return null if file does not exist
    File file = SegmentDirectoryPaths.findVectorIndexIndexFile(segmentIndexDir, _column);
    if (file == null) {
      throw new IllegalStateException("Failed to find text index file for column: " + _column);
    }
    return file;
  }

  @Override
  public MutableRoaringBitmap getDocIds(float[] searchQuery, int topK) {
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    Collector docIDCollector = new HnswDocIdCollector(docIds, _docIdTranslator);
    try {
      // Lucene Query Parser is JavaCC based. It is stateful and should
      // be instantiated per query. Analyzer on the other hand is stateless
      // and can be created upfront.
      QueryParser parser = new QueryParser(_column, null);

      if (_useANDForMultiTermQueries) {
        parser.setDefaultOperator(QueryParser.Operator.AND);
      }
      KnnFloatVectorQuery knnFloatVectorQuery = new KnnFloatVectorQuery(_column, searchQuery, topK);
      _indexSearcher.search(knnFloatVectorQuery, docIDCollector);
      return docIds;
    } catch (Exception e) {
      String msg =
          "Caught excepttion while searching the text index for column:" + _column + " search query:" + searchQuery;
      throw new RuntimeException(msg, e);
    }
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
    _docIdTranslator.close();
  }

  /**
   * Lucene docIDs are not same as pinot docIDs. The internal implementation
   * of Lucene can change the docIds and they are not guaranteed to be the
   * same as how we expect -- strictly increasing docIDs as the documents
   * are ingested during segment/index creation.
   * This class is used to map the luceneDocId (returned by the search query
   * to the collector) to corresponding pinotDocId.
   */
  static class DocIdTranslator implements Closeable {
    final PinotDataBuffer _buffer;

    DocIdTranslator(File segmentIndexDir, String column, int numDocs, IndexSearcher indexSearcher)
        throws Exception {
      int length = Integer.BYTES * numDocs;
      File docIdMappingFile = new File(SegmentDirectoryPaths.findSegmentDirectory(segmentIndexDir),
          column + V1Constants.Indexes.VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION);
      // The mapping is local to a segment. It is created on the server during segment load.
      // Unless we are running Pinot on Solaris/SPARC, the underlying architecture is
      // LITTLE_ENDIAN (Linux/x86). So use that as byte order.
      String desc = "Text index docId mapping buffer: " + column;
      if (docIdMappingFile.exists()) {
        // we will be here for segment reload and server restart
        // for refresh, we will not be here since segment is deleted/replaced
        // TODO: see if we can prefetch the pages
        _buffer =
            PinotDataBuffer.mapFile(docIdMappingFile, /* readOnly */ true, 0, length, ByteOrder.LITTLE_ENDIAN, desc);
      } else {
        _buffer =
            PinotDataBuffer.mapFile(docIdMappingFile, /* readOnly */ false, 0, length, ByteOrder.LITTLE_ENDIAN, desc);
        for (int i = 0; i < numDocs; i++) {
          try {
            Document document = indexSearcher.doc(i);
            int pinotDocId = Integer.parseInt(document.get(HnswVectorIndexCreator.VECTOR_INDEX_DOC_ID_COLUMN_NAME));
            _buffer.putInt(i * Integer.BYTES, pinotDocId);
          } catch (Exception e) {
            throw new RuntimeException(
                "Caught exception while building doc id mapping for text index column: " + column, e);
          }
        }
      }
    }

    int getPinotDocId(int luceneDocId) {
      return _buffer.getInt(luceneDocId * Integer.BYTES);
    }

    @Override
    public void close()
        throws IOException {
      _buffer.close();
    }
  }
}
