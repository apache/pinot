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
package org.apache.pinot.plugin.inputformat.protobuf;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Tests the last-known-good fallback for descriptor files on remote filesystems: the descriptor is fetched fresh on
/// every call (so in-place updates propagate), and the last successfully fetched content is served only when the
/// fetch fails or returns unparseable bytes, so decoder creation survives transient DNS / object-store outages.
public class ProtoBufUtilsDescriptorCacheTest {
  private static final String COUNTING_SCHEME = "counting";

  private File _descriptorFile;
  private byte[] _descriptorContent;
  private byte[] _otherDescriptorContent;

  /// A "remote" filesystem that serves local files, counts downloads and can simulate network failures.
  public static class CountingPinotFS extends LocalPinotFS {
    static final AtomicInteger COPY_CALLS = new AtomicInteger();
    static volatile boolean _failCopies = false;

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws Exception {
      COPY_CALLS.incrementAndGet();
      if (_failCopies) {
        throw new IOException("Simulated network failure: Temporary failure in name resolution");
      }
      super.copyToLocalFile(srcUri, dstFile);
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    PinotFSFactory.register(COUNTING_SCHEME, CountingPinotFS.class.getName(), null);
    _descriptorFile = File.createTempFile("proto-descriptor-cache-test", ".desc");
    try (InputStream in = getClass().getClassLoader().getResourceAsStream("sample.desc")) {
      Files.copy(in, _descriptorFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
    _descriptorContent = Files.readAllBytes(_descriptorFile.toPath());
    try (InputStream in = getClass().getClassLoader().getResourceAsStream("complex_types.desc")) {
      _otherDescriptorContent = in.readAllBytes();
    }
  }

  @AfterClass
  public void tearDown() {
    _descriptorFile.delete();
  }

  @BeforeMethod
  public void reset()
      throws Exception {
    ProtoBufUtils.clearDescriptorCache();
    CountingPinotFS.COPY_CALLS.set(0);
    CountingPinotFS._failCopies = false;
    Files.write(_descriptorFile.toPath(), _descriptorContent);
  }

  private String remotePath() {
    return COUNTING_SCHEME + "://" + _descriptorFile.getAbsolutePath();
  }

  @Test
  public void testFetchesFreshOnEveryCall()
      throws Exception {
    assertEquals(readAll(ProtoBufUtils.getDescriptorFileInputStream(remotePath())), _descriptorContent);
    assertEquals(CountingPinotFS.COPY_CALLS.get(), 1);

    // An in-place update of the remote file must be picked up by the next fetch
    Files.write(_descriptorFile.toPath(), _otherDescriptorContent);
    assertEquals(readAll(ProtoBufUtils.getDescriptorFileInputStream(remotePath())), _otherDescriptorContent);
    assertEquals(CountingPinotFS.COPY_CALLS.get(), 2);
  }

  @Test
  public void testFallsBackToLastFetchedCopyOnFetchFailure()
      throws Exception {
    assertEquals(readAll(ProtoBufUtils.getDescriptorFileInputStream(remotePath())), _descriptorContent);

    CountingPinotFS._failCopies = true;
    assertEquals(readAll(ProtoBufUtils.getDescriptorFileInputStream(remotePath())), _descriptorContent);
    // The fetch was attempted (and failed) before falling back
    assertEquals(CountingPinotFS.COPY_CALLS.get(), 2);

    // Recovery: once the network is back, fresh content is fetched and remembered again
    CountingPinotFS._failCopies = false;
    Files.write(_descriptorFile.toPath(), _otherDescriptorContent);
    assertEquals(readAll(ProtoBufUtils.getDescriptorFileInputStream(remotePath())), _otherDescriptorContent);
  }

  @Test
  public void testFetchFailureWithoutFallbackCopyPropagates() {
    CountingPinotFS._failCopies = true;
    assertThrows(IOException.class, () -> ProtoBufUtils.getDescriptorFileInputStream(remotePath()));
  }

  @Test
  public void testCorruptDownloadDoesNotPoisonFallbackCopy()
      throws Exception {
    assertEquals(readAll(ProtoBufUtils.getDescriptorFileInputStream(remotePath())), _descriptorContent);

    // Overwrite the remote file with bytes that cannot be parsed as a descriptor set (0xFF = invalid wire type).
    // The corrupt content must neither be served nor replace the last known good copy.
    byte[] corrupt = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    Files.write(_descriptorFile.toPath(), corrupt);
    assertEquals(readAll(ProtoBufUtils.getDescriptorFileInputStream(remotePath())), _descriptorContent);

    // A later valid update is picked up normally
    Files.write(_descriptorFile.toPath(), _otherDescriptorContent);
    assertEquals(readAll(ProtoBufUtils.getDescriptorFileInputStream(remotePath())), _otherDescriptorContent);
  }

  @Test
  public void testLocalDescriptorIsReadFreshAndNotRemembered()
      throws Exception {
    File localFile = File.createTempFile("proto-descriptor-cache-test-local", ".desc");
    try {
      Files.write(localFile.toPath(), _descriptorContent);
      assertEquals(readAll(ProtoBufUtils.getDescriptorFileInputStream(localFile.getAbsolutePath())),
          _descriptorContent);

      // A local file edited in place must be re-read
      Files.write(localFile.toPath(), _otherDescriptorContent);
      assertEquals(readAll(ProtoBufUtils.getDescriptorFileInputStream(localFile.getAbsolutePath())),
          _otherDescriptorContent);
      assertEquals(CountingPinotFS.COPY_CALLS.get(), 0);
    } finally {
      localFile.delete();
    }
  }

  private static byte[] readAll(InputStream inputStream)
      throws Exception {
    try (InputStream in = inputStream) {
      return in.readAllBytes();
    }
  }
}
