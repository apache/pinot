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
package org.apache.pinot.segment.local.segment.index.forward.mutable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CLPMutableForwardIndexV2Test implements PinotBuffersAfterClassCheckRule {
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(VarByteSVMutableForwardIndexTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  /**
   * Sanity check
   */
  @Test
  public void testReadWriteOnLogMessages()
      throws IOException {
    try (CLPMutableForwardIndexV2 readerWriter = new CLPMutableForwardIndexV2("col1", _memoryManager)) {
      List<String> logLines = new ArrayList<>();
      for (int i = 0; i < 10000; i++) {
        logLines.add("INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32c_DEFAULT)] "
            + "Event pinot::DEFAULT::4a02a32c_DEFAULT : Refreshed 35 property LiveInstance took 5 ms. Selective:"
            + " true");
        logLines.add("INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32d_DEFAULT)] "
            + "Event pinot::DEFAULT::4a02a32d_DEFAULT : Refreshed 81 property LiveInstance took 4 ms. Selective:"
            + " true");
        logLines.add("INFO [ControllerResponseFilter] [grizzly-http-server-2] Handled request from 0.0"
            + ".0.0 GET https://0.0.0.0:8443/health?checkType=liveness, content-type null status code 200 OK");
        logLines.add("INFO [ControllerResponseFilter] [grizzly-http-server-6] Handled request from 0.0"
            + ".0.0 GET https://pinot-pinot-broker-headless.managed.svc.cluster.local:8093/tables, content-type "
            + "application/json status code 200 OK");
        logLines.add("null");
      }

      // Typically, log messages should be clp encoded due to low logtype and dictionary variable cardinality
      Assert.assertTrue(readerWriter.isClpEncoded());

      // Write
      for (int i = 0; i < logLines.size(); i++) {
        readerWriter.setString(i, logLines.get(i));
      }

      // Read
      for (int i = 0; i < logLines.size(); i++) {
        Assert.assertEquals(readerWriter.getString(i), logLines.get(i));
      }
    }
  }

  @Test
  public void testClpDictionaryCompression()
      throws IOException {
    try (CLPMutableForwardIndexV2 readerWriter = new CLPMutableForwardIndexV2("col1", _memoryManager)) {
      // Write 400,000 logs
      // Mutable index should containing 4 unique logtype, 5 unique dictionary variables and 200,000 encoded values
      for (int i = 0; i < 4 * 100000; i += 4) {
        readerWriter.setString(i, "static value, dictionaryVar" + i % 5 + ", encodedVar: " + i);
        readerWriter.setString(i + 1, "static value, dictionaryVar" + i % 5);
        readerWriter.setString(i + 2, "static value, encodedVar: " + i);
        readerWriter.setString(i + 3, "static value");
      }

      // Mutable forward index should be clp encoded since cardinality is low
      Assert.assertTrue(readerWriter.isClpEncoded());

      // Mutable forward index should contain exactly 400,000 documents
      Assert.assertEquals(readerWriter.getNumDoc(), 400000);

      // Mutable forward index should contain exactly 4 unique logtype
      Assert.assertEquals(readerWriter.getLogtypeDict().length(), 4);

      // Mutable forward index should contain exactly 5 unique dictionary variables
      Assert.assertEquals(readerWriter.getDictVarDict().length(), 5);

      // Mutable forward index should contain exactly 400,000 encoded values
      Assert.assertEquals(readerWriter.getNumEncodedVar(), 200000);
    }
  }

  @Test
  public void testRawEncodingDueToHighLogtypeCardinality()
      throws IOException {
    try (CLPMutableForwardIndexV2 readerWriter = new CLPMutableForwardIndexV2("col1", _memoryManager)) {
      // Write 400,000 logs
      // Mutable index should containing 400,000 unique logtype
      for (int i = 0; i < 4 * 100000; i++) {
        String log = generateRandomString(64);
        readerWriter.setString(i, log);
        Assert.assertEquals(readerWriter.getString(i), log);
      }

      // Mutable forward index should be clp encoded since cardinality is low
      Assert.assertFalse(readerWriter.isClpEncoded());
    }
  }

  @Test
  public void testRawEncodingDueToHighDictVarCardinality()
      throws IOException {
    // Define the character set (A-Z and a-z)
    try (CLPMutableForwardIndexV2 readerWriter = new CLPMutableForwardIndexV2("col1", _memoryManager)) {
      // Write 400,000 logs
      // Mutable index should containing 1 unique logtype, 400,000 unique dictVar values
      for (int i = 0; i < 4 * 100000; i++) {
        String log = "A log with " + generateRandomString(64) + "-" + i;
        readerWriter.setString(i, log);
        Assert.assertEquals(readerWriter.getString(i), log);
      }

      // Mutable forward index should be clp encoded since cardinality is low
      Assert.assertFalse(readerWriter.isClpEncoded());
    }
  }

  private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  private static String generateRandomString(int length) {
    StringBuilder result = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      // Pick a random character from CHARACTERS string
      int index = ThreadLocalRandom.current().nextInt(CHARACTERS.length());
      result.append(CHARACTERS.charAt(index));
    }
    return result.toString();
  }
}
