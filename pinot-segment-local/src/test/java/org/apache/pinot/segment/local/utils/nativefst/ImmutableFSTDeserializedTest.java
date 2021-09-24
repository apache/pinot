package org.apache.pinot.segment.local.utils.nativefst;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.regexQueryNrHits;
import static org.testng.Assert.assertEquals;


/**
 * Deserialize a FST and ensure results are right
 */
public class ImmutableFSTDeserializedTest {
  private FST _fst;

  @BeforeTest
  public void setUp()
      throws Exception {
    InputStream fileInputStream = null;
    File file = new File("./src/test/resources/data/serfst.txt");

    fileInputStream = new FileInputStream(file);

    _fst = FST.read(fileInputStream, true, new DirectMemoryManager(ImmutableFSTDeserializedTest.class.getName()));
  }

  @Test
  public void testRegex1() {
    assertEquals(705, regexQueryNrHits("a.*", _fst));
  }

  @Test
  public void testRegex3() {
    assertEquals(52, regexQueryNrHits(".*a", _fst));
  }

  @Test
  public void testRegex4() {
    assertEquals(1004, regexQueryNrHits("~#", _fst));
  }
}
