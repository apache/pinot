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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTInfo;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pinot.segment.local.utils.nativefst.FSTFlags.NEXTBIT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Additional tests for {@link ImmutableFST}.
 */
public final class ImmutableFSTTest {
  public List<String> _expected = Arrays.asList("a", "aba", "ac", "b", "ba", "c");

  @Test
  public void testVersion5() throws IOException {
    File file = new File("./src/test/resources/data/abc.fsa");
    final FST FST = org.apache.pinot.segment.local.utils.nativefst.FST.read(new FileInputStream(file));
    assertFalse(FST.getFlags().contains(FSTFlags.NUMBERS));
    verifyContent(_expected, FST);
  }

  @Test
  public void testVersion5WithNumbers() throws IOException {
    File file = new File("./src/test/resources/data/abc-numbers.fsa");
    final FST FST = org.apache.pinot.segment.local.utils.nativefst.FST.read(new FileInputStream(file));

    verifyContent(_expected, FST);
    assertTrue(FST.getFlags().contains(FSTFlags.NUMBERS));
  }

  @Test
  public void testArcsAndNodes() throws IOException {
    File file = new File("./src/test/resources/data/abc.fsa");
    final FST FST1 = FST.read(new FileInputStream(file));

    file = new File("./src/test/resources/data/abc-numbers.fsa");
    final FST FST2 = FST.read(new FileInputStream(file));

    FSTInfo info1 = new FSTInfo(FST1);
    FSTInfo info2 = new FSTInfo(FST2);

    assertEquals(info1.arcsCount, info2.arcsCount);
    assertEquals(info1.nodeCount, info2.nodeCount);

    assertEquals(4, info2.nodeCount);
    assertEquals(7, info2.arcsCount);
  }

  @Test
  public void testNumbers() throws IOException {
    File file = new File("./src/test/resources/data/abc-numbers.fsa");
    final FST FST = org.apache.pinot.segment.local.utils.nativefst.FST.read(new FileInputStream(file));

    assertTrue(FST.getFlags().contains(NEXTBIT));

    // Get all numbers for nodes.
    byte[] buffer = new byte[128];
    final ArrayList<String> result = new ArrayList<String>();
    walkNode(buffer, 0, FST, FST.getRootNode(), 0, result);

    Collections.sort(result);
    assertEquals(Arrays.asList("0 c", "1 b", "2 ba", "3 a", "4 ac", "5 aba"), result);
  }

  public static void walkNode(byte[] buffer, int depth, FST FST, int node, int cnt, List<String> result)
      throws IOException {
    for (int arc = FST.getFirstArc(node); arc != 0; arc = FST.getNextArc(arc)) {
      buffer[depth] = FST.getArcLabel(arc);

      if (FST.isArcFinal(arc) || FST.isArcTerminal(arc)) {
        result.add(cnt + " " + new String(buffer, 0, depth + 1, "UTF-8"));
      }

      if (FST.isArcFinal(arc)) {
        cnt++;
      }

      if (!FST.isArcTerminal(arc)) {
        walkNode(buffer, depth + 1, FST, FST.getEndNode(arc), cnt, result);
        cnt += FST.getRightLanguageCount(FST.getEndNode(arc));
      }
    }
  }

  private static void verifyContent(List<String> expected, FST FST) throws IOException {
    final ArrayList<String> actual = new ArrayList<String>();

    int count = 0;
    for (ByteBuffer bb : FST.getSequences()) {
      assertEquals(0, bb.arrayOffset());
      assertEquals(0, bb.position());
      actual.add(new String(bb.array(), 0, bb.remaining(), "UTF-8"));
      count++;
    }
    assertEquals(expected.size(), count);
    Collections.sort(actual);
    assertEquals(expected, actual);
  }

  @Test
  public void testSave() throws IOException {
    List<String> inputList = List.of("aeh", "pfh");
    FSTBuilder builder = new FSTBuilder();

    for (int i = 0; i < inputList.size(); i++) {
      builder.add(inputList.get(i).getBytes(UTF_8), 0, inputList.get(i).length(), 127);
    }

    FST FST = builder.complete();

    final File writeFile =  new File(FileUtils.getTempDirectory(), "ImmutableFSTTest");

    FST.save(new FileOutputStream(writeFile));

    final FST readFST = FST.read(new FileInputStream(writeFile), ImmutableFST.class, true);

    verifyContent(inputList, readFST);
  }
}
