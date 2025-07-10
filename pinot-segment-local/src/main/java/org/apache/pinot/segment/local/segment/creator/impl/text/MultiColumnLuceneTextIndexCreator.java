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
package org.apache.pinot.segment.local.segment.creator.impl.text;

import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.LuceneNRTCachingMergePolicy;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader.ColumnConfig;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextIndexConstants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used to create Lucene-based multi-column text index.
 * Used for both offline from {@link SegmentColumnarIndexCreator}
 * and realtime from {@link RealtimeLuceneTextIndex}
 * While this is an index creator, it consumes values for multiple columns in a single call,
 * which doesn't fit IndexCreator.
 */
public class MultiColumnLuceneTextIndexCreator implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiColumnLuceneTextIndexCreator.class);
  public static final String LUCENE_INDEX_DOC_ID_COLUMN_NAME = "DocID";

  private final List<String> _textColumns;
  // per-column isSingleValue boolean flag
  private final BooleanList _textColumnsSV;
  private final boolean _commitOnClose;
  private final boolean _reuseMutableIndex;
  private final File _indexFile;
  private Directory _indexDirectory;
  private IndexWriter _indexWriter;
  private int _nextDocId = 0;

  /**
   * Called by {@link SegmentColumnarIndexCreator}
   * when building an offline segment. Similar to how it creates per column
   * dictionary, forward and inverted index, a text index is also created
   * if text search is enabled on a column.
   * @param columns column names
   * @param columnsSV per-column flags, true->single value, false-> multi-value
   * @param segmentIndexDir segment index directory
   * @param commit true if the index should be committed (at the end after all documents have
   *               been added), false if index should not be committed
   * @param realtimeConversion index creator should create an index using the realtime segment
   * @param consumerDir consumer dir containing the realtime index, used when realtimeConversion and commit is true
   * @param immutableToMutableIdMap immutableToMutableIdMap from segment conversion
   * Note on commit:
   *               Once {@link SegmentColumnarIndexCreator}
   *               finishes indexing all documents/rows for the segment, we need to commit and close
   *               the Lucene index which will internally persist the index on disk, do the necessary
   *               resource cleanup etc. We commit during {@link DictionaryBasedInvertedIndexCreator#seal()}
   *               and close during {@link DictionaryBasedInvertedIndexCreator#close()}.
   *               This lucene index writer is used by both offline and realtime (both during
   *               indexing in-memory MutableSegment and later during conversion to offline).
   *               Since realtime segment conversion is again going to go through the offline
   *               indexing path and will do everything (indexing, commit, close etc), there is
   *               no need to commit the index from the realtime side. So when the realtime segment
   *               is destroyed (which is after the realtime segment has been committed and converted
   *               to offline), we close this lucene index writer to release resources but don't commit.
   * @param mcTextConfig the text index config
   */
  public MultiColumnLuceneTextIndexCreator(
      List<String> columns,
      BooleanList columnsSV,
      File segmentIndexDir,
      boolean commit,
      boolean realtimeConversion,
      @Nullable File consumerDir,
      @Nullable int[] immutableToMutableIdMap,
      MultiColumnTextIndexConfig mcTextConfig) {
    _textColumns = columns;
    _textColumnsSV = columnsSV;
    _commitOnClose = commit;

    TextIndexConfig config = new TextIndexConfigBuilder().withProperties(mcTextConfig.getProperties()).build();

    String luceneAnalyzerClass = config.getLuceneAnalyzerClass();
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      _indexFile = getV1TextIndexFile(segmentIndexDir);

      // write properties file for the immutable segment
      if (_commitOnClose) {
        TextIndexUtils.writeConfigToPropertiesFile(_indexFile, config);
      }

      Map<String, ColumnConfig> perColConfigs =
          MultiColumnLuceneTextIndexReader.buildColumnConfigs(mcTextConfig.getPerColumnProperties());

      Analyzer luceneAnalyzer = MultiColumnLuceneTextIndexReader.buildAnalyzer(config, perColConfigs);
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(luceneAnalyzer);
      indexWriterConfig.setRAMBufferSizeMB(config.getLuceneMaxBufferSizeMB());
      indexWriterConfig.setCommitOnClose(commit);
      indexWriterConfig.setUseCompoundFile(config.isLuceneUseCompoundFile());

      // Also, for the realtime segment, we set the OpenMode to CREATE to ensure that any existing artifacts
      // will be overwritten. This is necessary because the realtime segment can be created multiple times
      // during a server crash and restart scenario. If the existing artifacts are appended to, the realtime
      // query results will be accurate, but after segment conversion the mapping file generated will be loaded
      // for only the first numDocs lucene docIds, which can cause IndexOutOfBounds errors.
      if (!_commitOnClose && config.isReuseMutableIndex()) {
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      }

      // to reuse the mutable index, it must be (1) not the realtime index, i.e. commit is set to true
      // and (2) happens during realtime segment conversion
      // TODO: it'd be better to extract to separate implementation because when flag is true, it's basically a no-op
      // TODO: it's inefficient because implementation (SegmentIndexCreationDriver) still needs to iterate over rows,
      // TODO: only to check flag and do nothing. It'd be better to skip iteration altogether.
      _reuseMutableIndex = config.isReuseMutableIndex() && commit && realtimeConversion;
      if (_reuseMutableIndex) {
        LOGGER.info("Reusing the realtime lucene index for segment {} and column [{}]", segmentIndexDir, columns);
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        convertMutableSegment(segmentIndexDir, consumerDir, immutableToMutableIdMap, indexWriterConfig);
        return;
      }

      if (!_commitOnClose && config.getLuceneNRTCachingDirectoryMaxBufferSizeMB() > 0) {
        // For realtime index, use NRTCachingDirectory to reduce the number of open files. This buffers the
        // flushes triggered by the near real-time refresh and writes them to disk when the buffer is full,
        // reducing the number of small writes.
        int bufSize = config.getLuceneNRTCachingDirectoryMaxBufferSizeMB();
        LOGGER.info(
            "Using NRTCachingDirectory for realtime lucene index for segment {} and columns [{}] with buffer size: "
                + "{}MB",
            segmentIndexDir, columns, bufSize);
        NRTCachingDirectory dir = new NRTCachingDirectory(FSDirectory.open(_indexFile.toPath()), bufSize, bufSize);
        indexWriterConfig.setMergePolicy(new LuceneNRTCachingMergePolicy(dir));
        _indexDirectory = dir;
      } else {
        // LogByteSizeMergePolicy doesn't seem to guarantee that document order is preserved,
        // but it produces sequential ids way more often than default TieredMergePolicy
        // This is used by StarTree Json index
        if (config.isUseLogByteSizeMergePolicy()) {
          indexWriterConfig.setMergePolicy(new LogByteSizeMergePolicy());
        }

        _indexDirectory = FSDirectory.open(_indexFile.toPath());
      }
      _indexWriter = new IndexWriter(_indexDirectory, indexWriterConfig);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(
          "Failed to instantiate " + luceneAnalyzerClass + " lucene analyzer for column: " + columns, e);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while instantiating the LuceneTextIndexCreator for column: " + columns, e);
    }
  }

  public IndexWriter getIndexWriter() {
    return _indexWriter;
  }

  /**
   * Copy the mutable lucene index files to create an immutable lucene index
   * @param segmentIndexDir segment index directory
   * @param immutableToMutableIdMap immutableToMutableIdMap from segment conversion
   * @param indexWriterConfig indexWriterConfig
   */
  private void convertMutableSegment(File segmentIndexDir, File consumerDir, @Nullable int[] immutableToMutableIdMap,
      IndexWriterConfig indexWriterConfig) {
    try {
      // Copy the mutable index to the v1 index location
      File dest = getV1TextIndexFile(segmentIndexDir);
      File mutableDir = getMutableIndexDir(segmentIndexDir, consumerDir);
      FileUtils.copyDirectory(mutableDir, dest);

      // Remove the copied write.lock file
      File writeLock = new File(dest, "write.lock");
      FileUtils.delete(writeLock);

      // Call .forceMerge(1) on the copied index as the mutable index will likely contain many Lucene segments
      try (Directory destDirectory = FSDirectory.open(dest.toPath());
          IndexWriter indexWriter = new IndexWriter(destDirectory, indexWriterConfig)) {
        indexWriter.forceMerge(1, true);
        indexWriter.commit();

        buildMappingFile(segmentIndexDir, _textColumns, destDirectory, immutableToMutableIdMap);
      } catch (Exception e) {
        throw new RuntimeException("Failed to build the mapping file during segment conversion: " + e);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert the mutable lucene index: " + e);
    }
  }

  /**
   * Generate the mapping file from mutable Pinot docId (stored within the Lucene index) to immutable Pinot docId using
   * the immutableToMutableIdMap from segment conversion
   * @param segmentIndexDir segment index directory
   * @param columns column name
   * @param directory directory of the index
   * @param immutableToMutableIdMap immutableToMutableIdMap from segment conversion
   */
  private void buildMappingFile(File segmentIndexDir, List<String> columns, Directory directory,
      @Nullable int[] immutableToMutableIdMap)
      throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);

    int numDocs = indexSearcher.getIndexReader().numDocs();
    int length = Integer.BYTES * numDocs;
    File docIdMappingFile = new File(SegmentDirectoryPaths.findSegmentDirectory(segmentIndexDir),
        MultiColumnTextIndexConstants.INDEX_DIR_NAME
            + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    String desc = "Text index docId mapping buffer: " + columns;
    try (PinotDataBuffer buffer = PinotDataBuffer.mapFile(docIdMappingFile, /* readOnly */ false, 0, length,
        ByteOrder.LITTLE_ENDIAN, desc)) {
      try {
        // If immutableToMutableIdMap is null, then docIds should not change between the mutable and immutable segments.
        // Therefore, the mapping file can be built without doing an additional docId conversion
        if (immutableToMutableIdMap == null) {
          for (int i = 0; i < numDocs; i++) {
            Document document = indexSearcher.doc(i);
            int pinotDocId =
                Integer.parseInt(document.get(MultiColumnLuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME));
            buffer.putInt(i * Integer.BYTES, pinotDocId);
          }
          return;
        }

        for (int i = 0; i < numDocs; i++) {
          Document document = indexSearcher.doc(i);
          int mutablePinotDocId =
              Integer.parseInt(document.get(MultiColumnLuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME));
          int immutablePinotDocId = immutableToMutableIdMap[mutablePinotDocId];
          buffer.putInt(i * Integer.BYTES, immutablePinotDocId);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Caught exception while building mutable to immutable doc id mapping for text index column: " + columns, e);
      }
    } finally {
      indexReader.close();
    }
  }

  /**
   * Adds given SV or MV documents to multi-column text index.
   * @param documents list of values for all text columns (either String or String[])
   */
  public void add(List<Object> documents) {
    if (_reuseMutableIndex) {
      return; // no-op
    }

    Document docToIndex = new Document();
    for (int i = 0, n = _textColumns.size(); i < n; i++) {
      if (_textColumnsSV.getBoolean(i)) {
        docToIndex.add(new TextField(_textColumns.get(i), (String) documents.get(i), Field.Store.NO));
      } else {
        String column = _textColumns.get(i);
        // check because in some cases (e.g. default mv column value) we get Object[]
        if (documents.get(i) instanceof String[]) {
          String[] values = (String[]) documents.get(i);
          for (String value : values) {
            docToIndex.add(new TextField(column, value, Field.Store.NO));
          }
        } else {
          Object[] values = (Object[]) documents.get(i);
          for (Object value : values) {
            docToIndex.add(new TextField(column, (String) value, Field.Store.NO));
          }
        }
      }
    }

    docToIndex.add(new StoredField(LUCENE_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));
    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the Lucene index for columns: " + _textColumns, e);
    }
  }

  /**
   * Adds given SV or MV documents to multi-column text index.
   * Same as add(List<Object> documents) but allows passing length for multivalued columns (and reuse arrays).
   * @param documents documents list of values for all text columns (either String or String[])
   * @param lengths number of items passed in i-th multivalued column
   */
  public void add(List<Object> documents, int[] lengths) {
    if (_reuseMutableIndex) {
      return; // no-op
    }

    assert documents.size() == lengths.length;

    Document docToIndex = new Document();
    for (int col = 0, n = _textColumns.size(); col < n; col++) {
      if (_textColumnsSV.getBoolean(col)) {
        docToIndex.add(new TextField(_textColumns.get(col), (String) documents.get(col), Field.Store.NO));
      } else {
        String column = _textColumns.get(col);
        // check because in some cases (e.g. default mv column value) we get Object[]
        if (documents.get(col) instanceof String[]) {
          String[] values = (String[]) documents.get(col);
          for (int val = 0, len = lengths[col]; val < len; val++) {
            docToIndex.add(new TextField(column, values[val], Field.Store.NO));
          }
        } else {
          Object[] values = (Object[]) documents.get(col);
          for (int val = 0, len = lengths[col]; val < len; val++) {
            docToIndex.add(new TextField(column, (String) values[val], Field.Store.NO));
          }
        }
      }
    }

    docToIndex.add(new StoredField(LUCENE_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));
    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the Lucene index for columns: " + _textColumns, e);
    }
  }

  //@Override
  public void seal() {
    if (_reuseMutableIndex) {
      return;  // no-op
    }
    try {
      // Do this one time operation of combining the multiple lucene index files (if any)
      // into a single index file. Based on flush threshold and size of data, Lucene
      // can create index in multiple files and then uses a merge criteria to decide
      // if a single compound file (similar to Pinot's V3 format) should be created
      // holding all the index data.
      // Depending on the size of data, flush threshold (during index building) and the
      // outcome of Lucene's internal merge criteria, the final lucene index can have
      // multiple files. Since these files will be kept opened during segment load/mmap,
      // we want to minimize the number of situations that can lead to "too many open files"
      // error.
      // Of course, there is a worst case where there could be a table with 20k segments
      // and each segment has 3 TEXT columns, thus 3 Lucene indexes. So even with a compound
      // file, we are likely to exhaust the number of open file descriptors. In future, we
      // should probably explore a global lucene index (a single index for all TEXT columns)
      // as opposed to per column index.
      _indexWriter.forceMerge(1);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while sealing the Lucene index for column: " + _textColumns, e);
    }
  }

  @Override
  public void close()
      throws IOException {
    if (_reuseMutableIndex) {
      return;  // no-op
    }
    try {
      // based on the commit flag set in IndexWriterConfig, this will decide to commit or not
      _indexWriter.close();
      _indexDirectory.close();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while closing the Lucene index for column: " + _textColumns, e);
    } finally {
      // remove leftover write.lock file, as well as artifacts from .commit() being called on the realtime index
      if (!_commitOnClose) {
        FileUtils.deleteQuietly(_indexFile);
      }
    }
  }

  private File getV1TextIndexFile(File indexDir) {
    String luceneIndexDirectory =
        MultiColumnTextIndexConstants.INDEX_DIR_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION;
    return new File(indexDir, luceneIndexDirectory);
  }

  private File getMutableIndexDir(File indexDir, File consumerDir) {
    String segmentName = getSegmentName(indexDir);
    return new File(new File(consumerDir, segmentName),
        MultiColumnTextIndexConstants.INDEX_DIR_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
  }

  private String getSegmentName(File indexDir) {
    // tmpSegmentName format: tmp-tableName__9__1__20240227T0254Z-1709002522086
    String tmpSegmentName = indexDir.getParentFile().getName();
    return tmpSegmentName.substring(tmpSegmentName.indexOf("tmp-") + 4, tmpSegmentName.lastIndexOf('-'));
  }

  public int getNumDocs() {
    return _nextDocId;
  }

  public Object2IntOpenHashMap getMapping() {
    Object2IntOpenHashMap<Object> mapping = new Object2IntOpenHashMap<>();
    mapping.defaultReturnValue(-1);

    for (int i = 0; i < _textColumns.size(); i++) {
      mapping.put(_textColumns.get(i), i);
    }

    return mapping;
  }
}
