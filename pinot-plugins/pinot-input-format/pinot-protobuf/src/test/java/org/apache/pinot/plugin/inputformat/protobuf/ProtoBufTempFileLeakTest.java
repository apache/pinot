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
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.getFieldsInSampleRecord;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.getSampleRecordMessage;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies that reading a protobuf descriptor file does not copy it to a local temporary file.
///
/// Both consumers of [ProtoBufUtils#openDescriptorFile] are configured with a remote (non-`file:`) URI backed by
/// [ClasspathPinotFS], which counts filesystem interactions. Each test asserts that the descriptor was streamed via
/// [org.apache.pinot.spi.filesystem.PinotFS#open] and never staged through `copyToLocalFile`, and that the consumer
/// closed the stream it was handed. Assertions are made against these counters rather than by scanning the shared
/// `java.io.tmpdir`, which races with any other test creating directories with the same prefix.
public class ProtoBufTempFileLeakTest {
  private static final String REMOTE_SCHEME = "proto-test-remote";
  private static final String SAMPLE_DESCRIPTOR_URI = REMOTE_SCHEME + ":///sample.desc";

  @BeforeClass
  public void setUp() {
    PinotFSFactory.register(REMOTE_SCHEME, ClasspathPinotFS.class.getName(), null);
  }

  @BeforeMethod
  public void resetCounters() {
    ClasspathPinotFS.reset();
  }

  /// [ProtoBufMessageDecoder#init] must stream the descriptor and close it, without staging a local copy.
  @Test
  public void testMessageDecoderInitStreamsDescriptorWithoutLocalCopy()
      throws Exception {
    Map<String, String> decoderProps = new HashMap<>();
    decoderProps.put(ProtoBufMessageDecoder.DESCRIPTOR_FILE_PATH, SAMPLE_DESCRIPTOR_URI);
    ProtoBufMessageDecoder decoder = new ProtoBufMessageDecoder();
    decoder.init(decoderProps, getFieldsInSampleRecord(), "");

    // Verify decoding works off the remotely-read descriptor
    GenericRow destination = new GenericRow();
    decoder.decode(getSampleRecordMessage().toByteArray(), destination);
    assertEquals(destination.getValue("email"), "foobar@hello.com");

    assertStreamedAndClosed();
  }

  /// [ProtoBufRecordReader#init] must stream the descriptor and close it, without staging a local copy.
  @Test
  public void testRecordReaderInitStreamsDescriptorWithoutLocalCopy()
      throws Exception {
    File tempDataDir = Files.createTempDirectory("protobuf-descriptor-test-data").toFile();
    try {
      File dataFile = new File(tempDataDir, "test.data");
      try (FileOutputStream out = new FileOutputStream(dataFile)) {
        getSampleRecordMessage().writeDelimitedTo(out);
      }

      ProtoBufRecordReaderConfig config = new ProtoBufRecordReaderConfig();
      config.setDescriptorFile(URI.create(SAMPLE_DESCRIPTOR_URI));

      try (ProtoBufRecordReader reader = new ProtoBufRecordReader()) {
        reader.init(dataFile, getFieldsInSampleRecord(), config);
        assertTrue(reader.hasNext());
        GenericRow row = reader.next(new GenericRow());
        assertEquals(row.getValue("email"), "foobar@hello.com");
      }
    } finally {
      FileUtils.deleteDirectory(tempDataDir);
    }

    assertStreamedAndClosed();
  }

  /// Asserts the descriptor was read exactly once via `open`, never staged through `copyToLocalFile`, and that
  /// every stream handed to the consumer was closed.
  private static void assertStreamedAndClosed() {
    assertEquals(ClasspathPinotFS.getCopyToLocalFileCount(), 0,
        "Descriptor must be streamed, not copied to a local temporary file");
    assertEquals(ClasspathPinotFS.getOpenCount(), 1, "Descriptor should be opened exactly once");
    assertEquals(ClasspathPinotFS.getClosedCount(), ClasspathPinotFS.getOpenCount(),
        "Every opened descriptor stream must be closed");
  }

  /// PinotFS that serves classpath resources and records how it was used. Counters are static because
  /// [PinotFSFactory] instantiates and wraps the filesystem itself, leaving no handle on the instance.
  public static class ClasspathPinotFS extends LocalPinotFS {
    private static final AtomicInteger OPEN_COUNT = new AtomicInteger();
    private static final AtomicInteger CLOSED_COUNT = new AtomicInteger();
    private static final AtomicInteger COPY_TO_LOCAL_FILE_COUNT = new AtomicInteger();

    static void reset() {
      OPEN_COUNT.set(0);
      CLOSED_COUNT.set(0);
      COPY_TO_LOCAL_FILE_COUNT.set(0);
    }

    static int getOpenCount() {
      return OPEN_COUNT.get();
    }

    static int getClosedCount() {
      return CLOSED_COUNT.get();
    }

    static int getCopyToLocalFileCount() {
      return COPY_TO_LOCAL_FILE_COUNT.get();
    }

    @Override
    public InputStream open(URI uri)
        throws IOException {
      OPEN_COUNT.incrementAndGet();
      return new FilterInputStream(openResource(uri)) {
        private boolean _closed;

        /// Counts at most once per stream: protobuf parsing closes the stream it consumes, and the caller's
        /// try-with-resources closes it again. Both are correct; the assertion cares that it was closed at all.
        @Override
        public void close()
            throws IOException {
          if (!_closed) {
            _closed = true;
            CLOSED_COUNT.incrementAndGet();
          }
          super.close();
        }
      };
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws Exception {
      COPY_TO_LOCAL_FILE_COUNT.incrementAndGet();
      try (InputStream in = openResource(srcUri)) {
        FileUtils.copyInputStreamToFile(in, dstFile);
      }
    }

    private static InputStream openResource(URI uri)
        throws IOException {
      String name = new File(uri.getPath()).getName();
      InputStream in = ClasspathPinotFS.class.getClassLoader().getResourceAsStream(name);
      if (in == null) {
        throw new IOException("Classpath resource not found: " + name);
      }
      return in;
    }
  }
}
