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

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
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
import org.apache.pinot.segment.local.segment.index.text.AbstractTextIndexCreator;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is used to create Lucene based text index.
 * Used for both offline from {@link SegmentColumnarIndexCreator}
 * and realtime from {@link RealtimeLuceneTextIndex}
 */
public class LuceneTextIndexCreator extends AbstractTextIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexCreator.class);
  public static final String LUCENE_INDEX_DOC_ID_COLUMN_NAME = "DocID";

  private final String _textColumn;
  private final boolean _commitOnClose;
  private final boolean _reuseMutableIndex;
  private final File _indexFile;
  private Directory _indexDirectory;
  private IndexWriter _indexWriter;
  private int _nextDocId = 0;

  public static HashSet<String> getDefaultEnglishStopWordsSet() {
    return new HashSet<>(
        Arrays.asList("a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
            "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "than", "there", "these", "they",
            "this", "to", "was", "will", "with", "those"));
  }

  public static final CharArraySet ENGLISH_STOP_WORDS_SET = new CharArraySet(getDefaultEnglishStopWordsSet(), true);

  /**
   * Called by {@link SegmentColumnarIndexCreator}
   * when building an offline segment. Similar to how it creates per column
   * dictionary, forward and inverted index, a text index is also created
   * if text search is enabled on a column.
   * @param column column name
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
   * @param config the text index config
   */
  public LuceneTextIndexCreator(String column, File segmentIndexDir, boolean commit, boolean realtimeConversion,
      @Nullable File consumerDir, @Nullable int[] immutableToMutableIdMap, TextIndexConfig config) {
    _textColumn = column;
    _commitOnClose = commit;

    String luceneAnalyzerClass = config.getLuceneAnalyzerClass();
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      _indexFile = getV1TextIndexFile(segmentIndexDir);

      // write properties file for the immutable segment
      if (_commitOnClose) {
        TextIndexUtils.writeConfigToPropertiesFile(_indexFile, config);
      }

      Analyzer luceneAnalyzer = TextIndexUtils.getAnalyzer(config);
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
      _reuseMutableIndex = config.isReuseMutableIndex() && commit && realtimeConversion;
      if (_reuseMutableIndex) {
        LOGGER.info("Reusing the realtime lucene index for segment {} and column {}", segmentIndexDir, column);
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
            "Using NRTCachingDirectory for realtime lucene index for segment {} and column {} with buffer size: {}MB",
            segmentIndexDir, column, bufSize);
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
          "Failed to instantiate " + luceneAnalyzerClass + " lucene analyzer for column: " + column, e);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while instantiating the LuceneTextIndexCreator for column: " + column, e);
    }
  }

  public LuceneTextIndexCreator(IndexCreationContext context, TextIndexConfig indexConfig) {
    this(context.getFieldSpec().getName(), context.getIndexDir(), context.isTextCommitOnClose(),
        context.isRealtimeConversion(), context.getConsumerDir(), context.getImmutableToMutableIdMap(), indexConfig);
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

        buildMappingFile(segmentIndexDir, _textColumn, destDirectory, immutableToMutableIdMap);
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
   * @param column column name
   * @param directory directory of the index
   * @param immutableToMutableIdMap immutableToMutableIdMap from segment conversion
   */
  private void buildMappingFile(File segmentIndexDir, String column, Directory directory,
      @Nullable int[] immutableToMutableIdMap)
      throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);

    int numDocs = indexSearcher.getIndexReader().numDocs();
    int length = Integer.BYTES * numDocs;
    File docIdMappingFile = new File(SegmentDirectoryPaths.findSegmentDirectory(segmentIndexDir),
        column + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    String desc = "Text index docId mapping buffer: " + column;
    try (PinotDataBuffer buffer = PinotDataBuffer.mapFile(docIdMappingFile, /* readOnly */ false, 0, length,
        ByteOrder.LITTLE_ENDIAN, desc)) {
      try {
        // If immutableToMutableIdMap is null, then docIds should not change between the mutable and immutable segments.
        // Therefore, the mapping file can be built without doing an additional docId conversion
        if (immutableToMutableIdMap == null) {
          for (int i = 0; i < numDocs; i++) {
            Document document = indexSearcher.doc(i);
            int pinotDocId = Integer.parseInt(document.get(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME));
            buffer.putInt(i * Integer.BYTES, pinotDocId);
          }
          return;
        }

        for (int i = 0; i < numDocs; i++) {
          Document document = indexSearcher.doc(i);
          int mutablePinotDocId =
              Integer.parseInt(document.get(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME));
          int immutablePinotDocId = immutableToMutableIdMap[mutablePinotDocId];
          buffer.putInt(i * Integer.BYTES, immutablePinotDocId);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Caught exception while building mutable to immutable doc id mapping for text index column: " + column, e);
      }
    } finally {
      indexReader.close();
    }
  }

  @Override
  public void add(String document) {
    if (_reuseMutableIndex) {
      return; // no-op
    }

    // text index on SV column
    Document docToIndex = new Document();
    docToIndex.add(new TextField(_textColumn, document, Field.Store.NO));
    docToIndex.add(new StoredField(LUCENE_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));
    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the Lucene index for column: " + _textColumn, e);
    }
  }

  @Override
  public void add(String[] documents, int length) {
    if (_reuseMutableIndex) {
      return; // no-op
    }

    Document docToIndex = new Document();

    // Whenever multiple fields with the same name appear in one document, both the
    // inverted index and term vectors will logically append the tokens of the
    // field to one another, in the order the fields were added.
    for (int i = 0; i < length; i++) {
      docToIndex.add(new TextField(_textColumn, documents[i], Field.Store.NO));
    }
    docToIndex.add(new StoredField(LUCENE_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));

    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the Lucene index for column: " + _textColumn, e);
    }
  }

  @Override
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
      throw new RuntimeException("Caught exception while sealing the Lucene index for column: " + _textColumn, e);
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
      throw new RuntimeException("Caught exception while closing the Lucene index for column: " + _textColumn, e);
    } finally {
      // remove leftover write.lock file, as well as artifacts from .commit() being called on the realtime index
      if (!_commitOnClose) {
        FileUtils.deleteQuietly(_indexFile);
      }
    }
  }

  private File getV1TextIndexFile(File indexDir) {
    String luceneIndexDirectory = _textColumn + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION;
    return new File(indexDir, luceneIndexDirectory);
  }

  private File getMutableIndexDir(File indexDir, File consumerDir) {
    String segmentName = getSegmentName(indexDir);
    return new File(new File(consumerDir, segmentName),
        _textColumn + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
  }

  private String getSegmentName(File indexDir) {
    // tmpSegmentName format: tmp-tableName__9__1__20240227T0254Z-1709002522086
    String tmpSegmentName = indexDir.getParentFile().getName();
    return tmpSegmentName.substring(tmpSegmentName.indexOf("tmp-") + 4, tmpSegmentName.lastIndexOf('-'));
  }

  public int getNumDocs() {
    return _nextDocId;
  }
}
