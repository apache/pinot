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
package org.apache.pinot.core.segment.creator.impl.text;

import java.io.File;
import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.core.segment.creator.TextIndexCreator;

public class LuceneTextIndexCreator implements TextIndexCreator {
  private static final int MAX_BUFFER_SIZE_MB = 500;
  public static final String DOC_ID_COLUMN_NAME = "DocID";
  public static final String DEFAULT_INDEX_FILE_EXTENSION = ".lucene.index";

  private final String _textColumn;
  private final IndexWriter _indexWriter;

  public LuceneTextIndexCreator(String column, File segmentIndexDir) {
    _textColumn = column;
    try {
      File indexFile = new File(segmentIndexDir.getPath() + "/" + _textColumn + DEFAULT_INDEX_FILE_EXTENSION);
      Directory indexDirectory = FSDirectory.open(indexFile.toPath());
      System.out.println("Lucene index file: " + indexFile.getAbsolutePath());
      StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(standardAnalyzer);
      indexWriterConfig.setRAMBufferSizeMB(MAX_BUFFER_SIZE_MB);
      _indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate Lucene text index creator. Error: " + e);
    }
  }

  @Override
  public void addDoc(Object document, int docIdCounter) {
    Document docToIndex = new Document();
    docToIndex.add(new TextField(_textColumn, document.toString(), Field.Store.NO));
    docToIndex.add(new StringField(DOC_ID_COLUMN_NAME, String.valueOf(docIdCounter), Field.Store.YES));
    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException("Failure while adding a new document to index. Error: " + e);
    }
  }

  @Override
  public void seal() {
    try {
      _indexWriter.commit();
    } catch (Exception e) {
      throw new RuntimeException("Lucene index writer failed to commit. Error: " + e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      _indexWriter.close();
    } catch (Exception e) {
      throw new RuntimeException("Lucene index writer failed to close. Error: " + e);
    }
  }
}
