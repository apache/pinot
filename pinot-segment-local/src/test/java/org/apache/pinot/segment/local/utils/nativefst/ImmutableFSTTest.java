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
package org.apache.pinot.segment.local.utils.nativefst;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTInfo;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Additional tests for {@link ImmutableFST}.
 */
public final class ImmutableFSTTest {
  public List<String> _expected = Arrays.asList("a", "aba", "ac", "b", "ba", "c");

  public static void walkNode(byte[] buffer, int depth, FST fst, int node, int cnt, List<String> result) {
    for (int arc = fst.getFirstArc(node); arc != 0; arc = fst.getNextArc(arc)) {
      buffer[depth] = fst.getArcLabel(arc);

      if (fst.isArcFinal(arc) || fst.isArcTerminal(arc)) {
        result.add(cnt + " " + new String(buffer, 0, depth + 1, UTF_8));
      }

      if (fst.isArcFinal(arc)) {
        cnt++;
      }

      if (!fst.isArcTerminal(arc)) {
        walkNode(buffer, depth + 1, fst, fst.getEndNode(arc), cnt, result);
        cnt += fst.getRightLanguageCount(fst.getEndNode(arc));
      }
    }
  }

  private static void verifyContent(FST fst, List<String> expected) {
    List<String> actual = new ArrayList<>();
    for (ByteBuffer bb : fst.getSequences()) {
      assertEquals(0, bb.arrayOffset());
      assertEquals(0, bb.position());
      actual.add(new String(bb.array(), 0, bb.remaining(), UTF_8));
    }
    actual.sort(null);
    assertEquals(actual, expected);
  }

  @Test
  public void testVersion5()
      throws IOException {
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("data/abc.native.fst")) {
      FST fst = FST.read(inputStream, 0);
      assertFalse(fst.getFlags().contains(FSTFlags.NUMBERS));
      verifyContent(fst, _expected);
    }
  }

  @Test
  public void testVersion5WithNumbers()
      throws IOException {
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("data/abc-numbers.native.fst")) {
      FST fst = FST.read(inputStream, 0);
      assertTrue(fst.getFlags().contains(FSTFlags.NUMBERS));
      verifyContent(fst, _expected);
    }
  }

  @Test
  public void testArcsAndNodes()
      throws IOException {
    for (String resourceName : new String[]{"data/abc.native.fst", "data/abc-numbers.native.fst"}) {
      try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourceName)) {
        //TODO: atri
        FST fst = FST.read(inputStream, 0);
        FSTInfo fstInfo = new FSTInfo(fst);
        assertEquals(fstInfo._nodeCount, 4);
        assertEquals(fstInfo._arcsCount, 7);
      }
    }
  }

  @Test
  public void testNumbers()
      throws IOException {
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("data/abc-numbers.native.fst")) {
      //TODO: atri
      FST fst = FST.read(inputStream, 0);
      assertTrue(fst.getFlags().contains(FSTFlags.NEXTBIT));

      // Get all numbers for nodes.
      byte[] buffer = new byte[128];
      List<String> result = new ArrayList<>();
      walkNode(buffer, 0, fst, fst.getRootNode(), 0, result);

      result.sort(null);
      assertEquals(result, Arrays.asList("0 c", "1 b", "2 ba", "3 a", "4 ac", "5 aba"));
    }
  }

  @Test
  public void testSave()
      throws IOException {
    List<String> inputList = Arrays.asList("aeh", "pfh");

    FSTBuilder builder = new FSTBuilder();
    for (String input : inputList) {
      builder.add(input.getBytes(UTF_8), 0, input.length(), 127);
    }
    FST fst = builder.complete();

    File fstFile = new File(FileUtils.getTempDirectory(), "test.native.fst");
    fst.save(new FileOutputStream(fstFile));

    try (FileInputStream inputStream = new FileInputStream(fstFile)) {
      //TODO: Atri
      verifyContent(FST.read(inputStream, ImmutableFST.class, true, 0), inputList);
    }

    FileUtils.deleteQuietly(fstFile);
  }
}
