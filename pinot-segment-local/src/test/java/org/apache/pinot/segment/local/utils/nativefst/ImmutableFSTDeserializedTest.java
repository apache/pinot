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

import java.io.IOException;
import java.io.InputStream;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.regexQueryNrHits;
import static org.testng.Assert.assertEquals;


/**
 * Deserialize a FST and ensure results are right
 */
public class ImmutableFSTDeserializedTest implements PinotBuffersAfterClassCheckRule {
  private FST _fst;
  private DirectMemoryManager _memManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("data/serfst.txt")) {
      _memManager = new DirectMemoryManager(ImmutableFSTDeserializedTest.class.getName());
      _fst = FST.read(inputStream, true, _memManager);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    if (_memManager != null) {
      _memManager.close();
    }
  }

  @Test
  public void testRegex() {
    assertEquals(regexQueryNrHits("a.*", _fst), 705);
    assertEquals(regexQueryNrHits(".*a", _fst), 52);
    assertEquals(regexQueryNrHits("~#", _fst), 1004);
  }
}
