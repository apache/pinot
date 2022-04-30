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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FSTBuilderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "FST");

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    TEMP_DIR.mkdirs();
  }

  @Test
  public void testRegexMatch() {
    RegexpMatcher regexpMatcher = new RegexpMatcher("hello.*ld", null);
    Assert.assertTrue(regexpMatcher.match("helloworld"));
    Assert.assertTrue(regexpMatcher.match("helloworld"));
    Assert.assertTrue(regexpMatcher.match("helloasdfworld"));
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
    FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
    OutputStreamDataOutput d = new OutputStreamDataOutput(fileOutputStream);
    fst.save(d);
    fileOutputStream.close();

    Outputs<Long> outputs = PositiveIntOutputs.getSingleton();
    File fstFile = new File(outputFile.getAbsolutePath());

    PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.mapFile(fstFile, true, 0, fstFile.length(), ByteOrder.BIG_ENDIAN, "");
    PinotBufferIndexInput indexInput = new PinotBufferIndexInput(pinotDataBuffer, 0L, fstFile.length());
    FST<Long> readFST = new FST(indexInput, outputs, new OffHeapFSTStore());

    IntList results = new IntArrayList();
    RegexpMatcher.regexMatch("hello.*123", fst, results::add);
    Assert.assertEquals(results.size(), 1);
    Assert.assertEquals(results.getInt(0), 21);

    results.clear();
    RegexpMatcher.regexMatch(".*world", fst, results::add);
    Assert.assertEquals(results.size(), 1);
    Assert.assertEquals(results.getInt(0), 12);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
