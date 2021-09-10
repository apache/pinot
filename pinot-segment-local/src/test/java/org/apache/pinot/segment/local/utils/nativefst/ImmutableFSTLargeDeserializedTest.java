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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Deserialize a large FST and ensure results are right
 */
public class ImmutableFSTLargeDeserializedTest {
  private FST _FST;

  @BeforeTest
  public void setUp()
      throws Exception {
    InputStream fileInputStream = null;
    File file = new File("./src/test/resources/data/large_fsa_serialized.txt");

    fileInputStream = new FileInputStream(file);

    _FST = FST.read(fileInputStream, true, new DirectMemoryManager(ImmutableFSTLargeDeserializedTest.class.getName()));
  }

  @Test
  public void testRegex1()
      throws IOException {
    assertEquals(207, regexQueryNrHits("q.[aeiou]c.*"));
  }

  @Test
  public void testRegex3()
      throws IOException {
    assertEquals(20858, regexQueryNrHits("b.*"));
  }

  @Test
  public void testRegex4()
      throws IOException {
    assertEquals(1204544, regexQueryNrHits("~#"));
  }

  @Test
  public void testRandomWords()
      throws IOException {
    assertEquals(1, regexQueryNrHits("respuestas"));
    assertEquals(1, regexQueryNrHits("Berge"));
    assertEquals(1, regexQueryNrHits("\\@qwx198595"));
    assertEquals(1, regexQueryNrHits("popular"));
    assertEquals(1, regexQueryNrHits("Montella"));
    assertEquals(1, regexQueryNrHits("notably"));
    assertEquals(1, regexQueryNrHits("accepted"));
    assertEquals(1, regexQueryNrHits("challenging"));
    assertEquals(1, regexQueryNrHits("insurance"));
    assertEquals(1, regexQueryNrHits("Calls"));
    assertEquals(1, regexQueryNrHits("certified"));
    assertEquals(1, regexQueryNrHits(".*196169"));
    assertEquals(4290, regexQueryNrHits(".*wx.*"));
    assertEquals(1, regexQueryNrHits("keeps"));
    assertEquals(1, regexQueryNrHits("\\@qwx160430"));
    assertEquals(1, regexQueryNrHits("called"));
    assertEquals(1, regexQueryNrHits("Rid"));
    assertEquals(1, regexQueryNrHits("Computer"));
    assertEquals(1, regexQueryNrHits("\\@qwx871194"));
    assertEquals(1, regexQueryNrHits("control"));
    assertEquals(1, regexQueryNrHits("Gassy"));
    assertEquals(1, regexQueryNrHits("Nut"));
    assertEquals(1, regexQueryNrHits("Strangle"));
    assertEquals(1, regexQueryNrHits("ANYTHING"));
    assertEquals(1, regexQueryNrHits("RiverMusic"));
    assertEquals(1, regexQueryNrHits("\\@qwx420154"));
  }

  /**
   * Return all matches for given regex
   */
  private long regexQueryNrHits(String regex)
      throws IOException {
    List<Long> resultList = RegexpMatcher.regexMatch(regex, _FST);

    return resultList.size();
  }
}
