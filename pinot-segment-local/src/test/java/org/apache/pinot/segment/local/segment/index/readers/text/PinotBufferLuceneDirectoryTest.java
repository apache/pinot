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

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.pinot.segment.spi.memory.ByteBufferPinotBufferFactory;
import org.apache.pinot.segment.spi.memory.PinotBufferFactory;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.unsafe.UnsafePinotBufferFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Reads a Lucene index packed into a [PinotDataBuffer], as the combined text and vector index files are, through
/// [PinotBufferLuceneDirectory], for both buffer implementations and either byte order.
public class PinotBufferLuceneDirectoryTest {
  private static final int NUM_DOCS = 500;
  private static final int PADDING = 13;

  @DataProvider
  public Object[][] buffers() {
    // The ByteBuffer factory backs small column files, the Unsafe one backs files over 2 GB (or every file when
    // pinot.offheap.prioritize.bytebuffer=false)
    PinotBufferFactory byteBuffer = new ByteBufferPinotBufferFactory();
    PinotBufferFactory unsafe = new UnsafePinotBufferFactory();
    return new Object[][]{
        {byteBuffer, ByteOrder.BIG_ENDIAN}, {byteBuffer, ByteOrder.LITTLE_ENDIAN},
        {unsafe, ByteOrder.BIG_ENDIAN}, {unsafe, ByteOrder.LITTLE_ENDIAN}
    };
  }

  @Test(dataProvider = "buffers")
  public void testSearchesIndexPackedInBuffer(PinotBufferFactory factory, ByteOrder byteOrder)
      throws Exception {
    try (ByteBuffersDirectory source = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(source, new IndexWriterConfig(new StandardAnalyzer()))) {
        for (int i = 0; i < NUM_DOCS; i++) {
          Document document = new Document();
          document.add(new TextField("text", (i % 2 == 0 ? "even" : "odd") + " doc" + i, Field.Store.NO));
          document.add(new KnnFloatVectorField("vector", new float[]{i, 1, -i}, VectorSimilarityFunction.EUCLIDEAN));
          writer.addDocument(document);
        }
        writer.forceMerge(1);
      }

      // Pack every file at an odd offset, so views do not start on an aligned boundary
      Map<String, LuceneTextIndexHeader.FileInfo> fileMap = new HashMap<>();
      long size = PADDING;
      for (String name : source.listAll()) {
        long length = source.fileLength(name);
        fileMap.put(name, new LuceneTextIndexHeader.FileInfo(name, size, length));
        size += length + PADDING;
      }
      try (PinotDataBuffer buffer = factory.allocateDirect(size, byteOrder)) {
        for (LuceneTextIndexHeader.FileInfo file : fileMap.values()) {
          byte[] bytes = new byte[(int) file.getSize()];
          try (IndexInput input = source.openInput(file.getName(), IOContext.READONCE)) {
            input.readBytes(bytes, 0, bytes.length);
          }
          buffer.readFrom(file.getOffset(), bytes);
        }

        PinotBufferLuceneDirectory directory = new PinotBufferLuceneDirectory(buffer, fileMap, "text");
        for (LuceneTextIndexHeader.FileInfo file : fileMap.values()) {
          try (IndexInput expected = source.openInput(file.getName(), IOContext.READONCE);
              IndexInput actual = directory.openInput(file.getName(), IOContext.READONCE)) {
            assertEquals(actual.length(), expected.length());
            for (long i = 0; i < expected.length(); i++) {
              assertEquals(actual.readByte(), expected.readByte(), file.getName() + " at " + i);
            }
          }
        }
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          assertEquals(searcher.count(new TermQuery(new Term("text", "even"))), NUM_DOCS / 2);
          TopDocs nearest = searcher.search(new KnnFloatVectorQuery("vector", new float[]{42, 1, -42}, 3), 3);
          assertEquals(nearest.scoreDocs[0].doc, 42);
        }
      }
    }
  }
}
