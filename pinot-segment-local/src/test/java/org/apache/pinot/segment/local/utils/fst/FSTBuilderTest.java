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
package org.apache.pinot.segment.local.utils.fst;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.index.readers.LuceneFSTIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class FSTBuilderTest implements PinotBuffersAfterMethodCheckRule {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "FSTBuilderTest");

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testRegexMatch() {
    RegexpMatcher regexpMatcher = new RegexpMatcher("hello.*ld", null);
    assertTrue(regexpMatcher.match("helloworld"));
    assertTrue(regexpMatcher.match("helloworld"));
    assertTrue(regexpMatcher.match("helloasdfworld"));
    Assert.assertFalse(regexpMatcher.match("ahelloasdfworld"));
  }

  @Test
  public void testFSTBuilder()
      throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("hello-world", 12);
    x.put("hello-world123", 21);
    x.put("still", 123);

    FST<Long> fst = FSTBuilder.buildFST(x);
    File outputFile = new File(TEMP_DIR, "test.lucene");
    try (FileOutputStream outputStream = new FileOutputStream(outputFile);
        OutputStreamDataOutput dataOutput = new OutputStreamDataOutput(outputStream)) {
      fst.save(dataOutput, dataOutput);
    }

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.loadBigEndianFile(outputFile);
        LuceneFSTIndexReader reader = new LuceneFSTIndexReader(dataBuffer)) {
      ImmutableRoaringBitmap result = reader.getDictIds("hello.*123");
      assertEquals(result.getCardinality(), 1);
      assertTrue(result.contains(21));

      result = reader.getDictIds(".*world");
      assertEquals(result.getCardinality(), 1);
      assertTrue(result.contains(12));

      result = reader.getDictIds(".*");
      assertEquals(result.getCardinality(), 3);
      assertTrue(result.contains(12));
      assertTrue(result.contains(21));
      assertTrue(result.contains(123));
    }
  }

  @Test
  public void testRegexMatchHonorsTermination()
      throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    for (int i = 0; i < 1000; i++) {
      x.put(String.format("key-%06d", i), i);
    }
    FST<Long> fst = FSTBuilder.buildFST(x);
    File outputFile = new File(TEMP_DIR, "test_termination.lucene");
    try (FileOutputStream outputStream = new FileOutputStream(outputFile);
        OutputStreamDataOutput dataOutput = new OutputStreamDataOutput(outputStream)) {
      fst.save(dataOutput, dataOutput);
    }

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.loadBigEndianFile(outputFile);
        LuceneFSTIndexReader reader = new LuceneFSTIndexReader(dataBuffer);
        QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      Thread.currentThread().interrupt();
      assertThrows(EarlyTerminationException.class,
          () -> RegexpMatcher.regexMatch(".*", fst, new MutableRoaringBitmap()::add));

      // The reader should propagate the termination exception as-is instead of wrapping it
      Thread.currentThread().interrupt();
      assertThrows(EarlyTerminationException.class, () -> reader.getDictIds(".*"));
    } finally {
      // Clear the interrupt flag in case the matcher did not consume it
      Thread.interrupted();
    }
  }
}
