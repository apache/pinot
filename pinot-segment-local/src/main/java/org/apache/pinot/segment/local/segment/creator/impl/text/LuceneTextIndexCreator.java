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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.segment.index.text.AbstractTextIndexCreator;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;


/**
 * This is used to create Lucene based text index.
 * Used for both offline from {@link SegmentColumnarIndexCreator}
 * and realtime from {@link RealtimeLuceneTextIndex}
 */
public class LuceneTextIndexCreator extends AbstractTextIndexCreator {
  public static final String LUCENE_INDEX_DOC_ID_COLUMN_NAME = "DocID";

  private final String _textColumn;
  private final Directory _indexDirectory;
  private final IndexWriter _indexWriter;

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
   * @param stopWordsInclude the words to include in addition to the default stop word list
   * @param stopWordsExclude the words to exclude from the default stop word list
   */
  public LuceneTextIndexCreator(String column, File segmentIndexDir, boolean commit,
      @Nullable List<String> stopWordsInclude, @Nullable List<String> stopWordsExclude, boolean useCompoundFile,
      int maxBufferSizeMB) {
    _textColumn = column;
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      File indexFile = getV1TextIndexFile(segmentIndexDir);
      _indexDirectory = FSDirectory.open(indexFile.toPath());

      StandardAnalyzer standardAnalyzer =
          TextIndexUtils.getStandardAnalyzerWithCustomizedStopWords(stopWordsInclude, stopWordsExclude);
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(standardAnalyzer);
      indexWriterConfig.setRAMBufferSizeMB(maxBufferSizeMB);
      indexWriterConfig.setCommitOnClose(commit);
      indexWriterConfig.setUseCompoundFile(useCompoundFile);
      _indexWriter = new IndexWriter(_indexDirectory, indexWriterConfig);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while instantiating the LuceneTextIndexCreator for column: " + column, e);
    }
  }

  public LuceneTextIndexCreator(IndexCreationContext context, TextIndexConfig indexConfig) {
    this(context.getFieldSpec().getName(), context.getIndexDir(), context.isTextCommitOnClose(),
        indexConfig.getStopWordsInclude(), indexConfig.getStopWordsExclude(), indexConfig.isLuceneUseCompoundFile(),
        indexConfig.getLuceneMaxBufferSizeMB());
  }

  public IndexWriter getIndexWriter() {
    return _indexWriter;
  }

  @Override
  public void add(String document) {
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
    try {
      // based on the commit flag set in IndexWriterConfig, this will decide to commit or not
      _indexWriter.close();
      _indexDirectory.close();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while closing the Lucene index for column: " + _textColumn, e);
    }
  }

  private File getV1TextIndexFile(File indexDir) {
    String luceneIndexDirectory = _textColumn + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION;
    return new File(indexDir, luceneIndexDirectory);
  }
}
