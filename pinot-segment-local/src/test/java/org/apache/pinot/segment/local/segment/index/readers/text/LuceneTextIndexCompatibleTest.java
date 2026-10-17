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
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.TextIndexConfig.DocIdTranslatorMode;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class LuceneTextIndexCompatibleTest {
  private static final String COLUMN = "Text";
  private static final int[] PINOT_IDS = {2, 0, 3, 1};

  @Test
  public void testLucene80IndexReader()
      throws IOException {
    File indexPath =
        new File(LuceneTextIndexCompatibleTest.class.getClassLoader().getResource("data/lucene_80_index").getPath());
    try (LuceneTextIndexReader lucene80Index = new LuceneTextIndexReader("Text", indexPath, 1000, Map.of())) {
      assertNotNull(lucene80Index);
    }
  }

  @Test
  public void testDocValuesMappingDoesNotReadStoredDocuments()
      throws IOException {
    File segmentDir = Files.createTempDirectory("lucene-doc-values-mapping").toFile();
    try {
      File indexDir = writeMappedIndex(segmentDir, "all", false, false);
      try (Directory directory = FSDirectory.open(indexDir.toPath());
          DirectoryReader reader = DirectoryReader.open(directory);
          PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(PINOT_IDS.length * Integer.BYTES,
              ByteOrder.LITTLE_ENDIAN, "doc-id mapping test")) {
        assertTrue(reader.leaves().size() > 1);
        IndexSearcher searcher = spy(new IndexSearcher(reader));
        assertFalse(LuceneTextIndexReader.fillDocIdMapping(searcher, buffer, PINOT_IDS.length));
        for (int i = 0; i < PINOT_IDS.length; i++) {
          assertEquals(buffer.getInt(i * Integer.BYTES), PINOT_IDS[i]);
        }
        verify(searcher, never()).storedFields();
      }
    } finally {
      FileUtils.deleteDirectory(segmentDir);
    }
  }

  @DataProvider(name = "mappingFormats")
  public Object[][] mappingFormats() {
    return new Object[][]{
        {"none", false, DocIdTranslatorMode.Default, false},
        {"mixed", false, DocIdTranslatorMode.Default, false},
        {"all", false, DocIdTranslatorMode.Default, false},
        {"none", true, DocIdTranslatorMode.TryOptimize, false},
        {"mixed", true, DocIdTranslatorMode.TryOptimize, false},
        {"all", true, DocIdTranslatorMode.TryOptimize, false},
        {"all", false, DocIdTranslatorMode.TryOptimize, true}
    };
  }

  @Test(dataProvider = "mappingFormats")
  public void testLegacyAndMixedDocValuesMapping(String docValuesMode, boolean deleteDocument,
      DocIdTranslatorMode translatorMode, boolean sortIndex)
      throws IOException {
    File segmentDir = Files.createTempDirectory("lucene-mixed-doc-values").toFile();
    try {
      writeMappedIndex(segmentDir, docValuesMode, deleteDocument, sortIndex);
      TextIndexConfig config =
          new TextIndexConfigBuilder().withDocIdTranslatorMode(translatorMode.name()).build();
      // Document ids are permuted and span multiple leaves. A deletion must not shift the remaining mappings.
      try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, segmentDir, PINOT_IDS.length, config)) {
        assertEquals(reader.getDocIds("tokenzero", null).toArray(), new int[]{2});
        assertEquals(reader.getDocIds("tokentwo", null).toArray(), new int[]{3});
        assertEquals(reader.getDocIds("tokenthree", null).toArray(), new int[]{1});
        assertEquals(reader.getDocIds("matchall", null).toArray(),
            deleteDocument ? new int[]{1, 2, 3} : new int[]{0, 1, 2, 3});
      }
      // A reload uses the persisted mapping generated from whichever representation each leaf provided.
      try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, segmentDir, PINOT_IDS.length, config)) {
        assertEquals(reader.getDocIds("matchall", null).toArray(),
            deleteDocument ? new int[]{1, 2, 3} : new int[]{0, 1, 2, 3});
      }
    } finally {
      FileUtils.deleteDirectory(segmentDir);
    }
  }

  private static File writeMappedIndex(File segmentDir, String docValuesMode, boolean deleteDocument, boolean sortIndex)
      throws IOException {
    File indexDir = new File(segmentDir, COLUMN + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    String[] tokens = {"tokenzero", "tokenone", "tokentwo", "tokenthree"};
    IndexWriterConfig writerConfig = new IndexWriterConfig(new StandardAnalyzer());
    if (sortIndex) {
      writerConfig.setIndexSort(new Sort(new SortField("order", SortField.Type.INT, true)));
    } else {
      writerConfig.setMergePolicy(NoMergePolicy.INSTANCE);
    }
    try (Directory directory = FSDirectory.open(indexDir.toPath());
        IndexWriter writer = new IndexWriter(directory, writerConfig)) {
      for (int i = 0; i < PINOT_IDS.length; i++) {
        Document document = new Document();
        document.add(new TextField(COLUMN, "matchall " + tokens[i], Field.Store.NO));
        document.add(new StringField("key", tokens[i], Field.Store.NO));
        document.add(new StoredField(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME, PINOT_IDS[i]));
        if (docValuesMode.equals("all") || docValuesMode.equals("mixed") && i == 1) {
          document.add(
              new NumericDocValuesField(LuceneTextIndexCreator.PINOT_DOC_ID_DOC_VALUES_FIELD_NAME, PINOT_IDS[i]));
        }
        if (sortIndex) {
          document.add(new NumericDocValuesField("order", i));
        }
        writer.addDocument(document);
        if (i % 2 == 1) {
          writer.commit();
        }
      }
      if (deleteDocument) {
        writer.deleteDocuments(new Term("key", tokens[1]));
        writer.commit();
      }
      if (sortIndex) {
        writer.forceMerge(1);
      }
    }
    return indexDir;
  }
}
