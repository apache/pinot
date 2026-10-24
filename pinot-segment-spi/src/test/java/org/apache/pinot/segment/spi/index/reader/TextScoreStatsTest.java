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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class TextScoreStatsTest {

  @Test
  public void testJsonRoundTrip() {
    TextScoreStats stats = stats("title", 10, 9, 42, 37, "usb", 4, 6);
    stats.getOrCreateField("title").addTermStats("o'neil \"quoted\"", 1, 1);
    String json = stats.toJson();
    assertEquals(TextScoreStats.fromJson(json), stats);
    assertEquals(TextScoreStats.fromJson(json).toJson(), json);
    assertTrue(json.startsWith("{\"fields\":{\"title\":{\"maxDoc\":10,\"docCount\":9,\"sumTotalTermFreq\":42,"
        + "\"sumDocFreq\":37,\"terms\":{"), json);
  }

  @Test
  public void testMergeSumsCounters() {
    TextScoreStats merged = stats("title", 10, 9, 42, 37, "usb", 4, 6)
        .merge(stats("title", 5, 5, 20, 18, "usb", 1, 1))
        .merge(stats("descr", 3, 3, 9, 9, "hub", 2, 2));
    TextScoreStats.FieldStats title = merged.getField("title");
    assertEquals(title.getMaxDoc(), 15);
    assertEquals(title.getDocCount(), 14);
    assertEquals(title.getSumTotalTermFreq(), 62);
    assertEquals(title.getSumDocFreq(), 55);
    assertEquals(title.getTermStats("usb"), new long[]{5, 7});
    assertNull(title.getTermStats("hub"));
    assertEquals(merged.getField("descr").getTermStats("hub"), new long[]{2, 2});
  }

  @Test
  public void testEmpty() {
    assertEquals(new TextScoreStats().toJson(), "{\"fields\":{}}");
    assertTrue(TextScoreStats.fromJson("{\"fields\":{}}").getFields().isEmpty());
  }

  @Test
  public void testInvalidInput() {
    expectThrows(IllegalArgumentException.class, () -> TextScoreStats.fromJson("not json"));
    expectThrows(IllegalArgumentException.class, () -> TextScoreStats.fromJson("{}"));
    expectThrows(IllegalArgumentException.class,
        () -> TextScoreStats.fromJson("{\"fields\":{\"title\":{\"terms\":{\"usb\":[1]}}}}"));
  }

  private static TextScoreStats stats(String field, long maxDoc, long docCount, long sumTotalTermFreq,
      long sumDocFreq, String term, long docFreq, long totalTermFreq) {
    TextScoreStats stats = new TextScoreStats();
    TextScoreStats.FieldStats fieldStats = stats.getOrCreateField(field);
    fieldStats.addCollectionStats(maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
    fieldStats.addTermStats(term, docFreq, totalTermFreq);
    return stats;
  }
}
