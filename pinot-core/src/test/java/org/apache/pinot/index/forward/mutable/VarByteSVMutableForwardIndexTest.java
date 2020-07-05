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
package org.apache.pinot.index.forward.mutable;

import java.io.IOException;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.core.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class VarByteSVMutableForwardIndexTest {
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

  @Test
  public void testString()
      throws IOException {
    // use arbitrary cardinality and avg string length
    // we will test with complete randomness
    int initialCapacity = 5;
    int estimatedAvgStringLength = 30;
    try (VarByteSVMutableForwardIndex readerWriter = new VarByteSVMutableForwardIndex(DataType.STRING, _memoryManager,
        "StringColumn", initialCapacity, estimatedAvgStringLength)) {
      int rows = 1000;
      Random random = new Random();
      String[] data = new String[rows];

      for (int i = 0; i < rows; i++) {
        // generate a random string of length between 10 and 100
        int length = 10 + random.nextInt(100 - 10);
        data[i] = RandomStringUtils.randomAlphanumeric(length);
        readerWriter.setString(i, data[i]);
      }

      for (int i = 0; i < rows; i++) {
        Assert.assertEquals(readerWriter.getString(i), data[i]);
      }
    }
  }

  @Test
  public void testBytes()
      throws IOException {
    int initialCapacity = 5;
    int estimatedAvgStringLength = 30;
    try (VarByteSVMutableForwardIndex readerWriter = new VarByteSVMutableForwardIndex(DataType.STRING, _memoryManager,
        "StringColumn", initialCapacity, estimatedAvgStringLength)) {
      int rows = 1000;
      Random random = new Random();
      String[] data = new String[rows];

      for (int i = 0; i < rows; i++) {
        int length = 10 + random.nextInt(100 - 10);
        data[i] = RandomStringUtils.randomAlphanumeric(length);
        readerWriter.setBytes(i, StringUtil.encodeUtf8(data[i]));
      }

      for (int i = 0; i < rows; i++) {
        Assert.assertEquals(StringUtil.decodeUtf8(readerWriter.getBytes(i)), data[i]);
      }
    }
  }
}
