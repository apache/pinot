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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTSerializerImpl;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Tests with weird input
 */
public class FSTRegexpWithWeirdTest {
  private FST _fst;

  private static byte[][] convertToBytes(String[] strings) {
    byte[][] data = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++) {
      String string = strings[i];
      data[i] = string.getBytes(Charset.defaultCharset()); // you can chose charset
    }
    return data;
  }

  @BeforeTest
  public void setUp()
      throws IOException {
    String regexTestInputString = "@qwx196169";
    String[] splitArray = regexTestInputString.split("\\s+");
    byte[][] bytesArray = convertToBytes(splitArray);

    Arrays.sort(bytesArray, FSTBuilder.LEXICAL_ORDERING);

    FSTBuilder fstBuilder = new FSTBuilder();

    for (byte[] currentArray : bytesArray) {
      fstBuilder.add(currentArray, 0, currentArray.length, -1);
    }

    FST s = fstBuilder.complete();

    final byte[] fsaData =
        new FSTSerializerImpl().withNumbers().serialize(s, new ByteArrayOutputStream()).toByteArray();

    _fst = FST.read(new ByteArrayInputStream(fsaData), ImmutableFST.class, true);
  }

  @Test
  public void testRegex1()
      throws IOException {
    assertEquals(1, regexQueryNrHits(".*196169"));
  }

  /**
   * Return all matches for given regex
   */
  private long regexQueryNrHits(String regex) {
    List<Long> resultList = RegexpMatcher.regexMatch(regex, _fst);

    return resultList.size();
  }
}
