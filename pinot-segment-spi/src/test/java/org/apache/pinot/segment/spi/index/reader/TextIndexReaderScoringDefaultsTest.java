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
package org.apache.pinot.segment.spi.index.reader;

import javax.annotation.Nullable;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// The scoring methods of [TextIndexReader] and [MultiColumnTextIndexReader] fail with a clear message on readers
/// that do not implement relevance scoring, and a multi-column reader refuses the column-less variants.
public class TextIndexReaderScoringDefaultsTest {

  @Test
  public void testSingleColumnReaderWithoutScoring() {
    TextIndexReader reader = new NonScoringTextIndexReader();
    assertMessage(expectThrows(UnsupportedOperationException.class, () -> reader.getDocScores("usb", null)),
        "Relevance scoring is not supported by text index reader: NonScoringTextIndexReader");
    // Without global statistics the 3-argument variant delegates to the 2-argument one
    assertMessage(expectThrows(UnsupportedOperationException.class, () -> reader.getDocScores("usb", null, null)),
        "Relevance scoring is not supported by text index reader: NonScoringTextIndexReader");
    assertMessage(expectThrows(UnsupportedOperationException.class,
            () -> reader.getDocScores("usb", null, new TextScoreStats())),
        "Relevance scoring with global statistics is not supported by text index reader: NonScoringTextIndexReader");
    assertMessage(expectThrows(UnsupportedOperationException.class, () -> reader.getTextScoreStats("usb", null)),
        "Text score statistics are not supported by text index reader: NonScoringTextIndexReader");
  }

  @Test
  public void testMultiColumnReaderWithoutScoring() {
    MultiColumnTextIndexReader reader = new NonScoringMultiColumnTextIndexReader();
    String readerName = "NonScoringMultiColumnTextIndexReader";
    assertMessage(expectThrows(UnsupportedOperationException.class,
            () -> reader.getDocScores("title", "usb", (String) null)),
        "Relevance scoring is not supported by text index reader: " + readerName);
    assertMessage(expectThrows(UnsupportedOperationException.class,
            () -> reader.getDocScores("title", "usb", (String) null, null)),
        "Relevance scoring is not supported by text index reader: " + readerName);
    assertMessage(expectThrows(UnsupportedOperationException.class,
            () -> reader.getDocScores("title", "usb", null, new TextScoreStats())),
        "Relevance scoring with global statistics is not supported by text index reader: " + readerName);
    assertMessage(expectThrows(UnsupportedOperationException.class,
            () -> reader.getTextScoreStats("title", "usb", null)),
        "Text score statistics are not supported by text index reader: " + readerName);
  }

  @Test
  public void testMultiColumnReaderRequiresColumn() {
    MultiColumnTextIndexReader reader = new NonScoringMultiColumnTextIndexReader();
    assertMessage(expectThrows(UnsupportedOperationException.class, () -> reader.getDocScores("usb", null)),
        "Multi-column text index requires column name to score!");
    assertMessage(expectThrows(UnsupportedOperationException.class,
            () -> reader.getDocScores("usb", null, new TextScoreStats())),
        "Multi-column text index requires column name to score!");
    assertMessage(expectThrows(UnsupportedOperationException.class, () -> reader.getTextScoreStats("usb", null)),
        "Multi-column text index requires column name to collect statistics!");
  }

  private static void assertMessage(Exception e, String expected) {
    assertEquals(e.getMessage(), expected);
  }

  private static class NonScoringTextIndexReader implements TextIndexReader {
    @Override
    public ImmutableRoaringBitmap getDictIds(String searchQuery) {
      return new MutableRoaringBitmap();
    }

    @Override
    public MutableRoaringBitmap getDocIds(String searchQuery) {
      return new MutableRoaringBitmap();
    }

    @Override
    public void close() {
    }
  }

  private static class NonScoringMultiColumnTextIndexReader extends NonScoringTextIndexReader
      implements MultiColumnTextIndexReader {
    @Override
    public MutableRoaringBitmap getDocIds(String column, String searchQuery, @Nullable String optionsString) {
      return new MutableRoaringBitmap();
    }
  }
}
