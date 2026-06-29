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
package org.apache.pinot.spi.filesystem;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class NoClosePinotFSTest {

  /**
   * {@link NoClosePinotFS} must delegate the ranged-read methods to its delegate; otherwise anything obtained
   * via {@link PinotFSFactory} (which wraps every registered FS in a NoClosePinotFS) would report no ranged-read
   * support and throw on {@link PinotFS#openForRead}.
   */
  @Test
  public void testDelegatesRangedRead()
      throws Exception {
    File file = Files.createTempFile("noclose-ranged", ".bin").toFile();
    try {
      byte[] data = new byte[256];
      for (int i = 0; i < data.length; i++) {
        data[i] = (byte) i;
      }
      FileUtils.writeByteArrayToFile(file, data);

      NoClosePinotFS fs = new NoClosePinotFS(new LocalPinotFS());
      Assert.assertTrue(fs.supportsRangedRead(), "must delegate supportsRangedRead() to the wrapped FS");
      try (InputStream in = fs.openForRead(file.toURI(), 10, 20)) {
        Assert.assertEquals(in.readAllBytes(), Arrays.copyOfRange(data, 10, 30));
      }
    } finally {
      Assert.assertTrue(file.delete());
    }
  }
}
