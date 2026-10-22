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
package org.apache.pinot.controller.coordination;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Real archives exercise immutable retries, destination-schema validation and bounds before catalog publication.
public class NativeSegmentUploadStorageTest {
  private static final String BOUNDARY = "pinot-native-upload-test";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("source").addSingleValueDimension("id", DataType.INT).build();
  private static final TableConfig TABLE =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("destination").build();

  @Test
  public void testDurableIdenticalRetryAcrossStorageInstancesAndDestinationSchema() throws Exception {
    try (Fixture fixture = new Fixture()) {
      byte[] archive = fixture.archive();
      Content first = fixture.store(archive, SCHEMA);
      CloseAwareInput input = new CloseAwareInput(multipart(archive, false));
      Content repeated = new NativeSegmentUploadStorage(fixture._conf).store(input, BOUNDARY,
          fixture._history, fixture._tableId, TABLE, SCHEMA);
      assertEquals(repeated, first);
      assertTrue(input._closed, "The outer request owner must close the entity after the final bounded read");
      assertEquals(first.tableName(), "destination_OFFLINE", "v2 tableName overrides embedded source table");
      assertEquals(first.loader().pushTimeMs(), -1L);
      assertEquals(first.loader().dataCrc(), -1L);
      assertFalse(first.loader().useDataCrc());
      assertEquals(Files.readAllBytes(Path.of(URI.create(first.downloadUri()))), archive);
      Schema incompatible =
          new Schema.SchemaBuilder().setSchemaName("destination").addSingleValueDimension("id", DataType.LONG).build();
      expectThrows(IllegalArgumentException.class, () -> fixture.store(archive, incompatible));
      fixture.noStaging();
    }
  }

  @Test
  public void testCrcAndExistingObjectCorruptionNeverPublishesOrOverwrites() throws Exception {
    try (Fixture fixture = new Fixture()) {
      byte[] archive = fixture.archive();
      Content content = fixture.store(archive, SCHEMA);
      Path immutable = Path.of(URI.create(content.downloadUri()));
      Files.writeString(immutable, "corrupted");
      expectThrows(IllegalArgumentException.class, () -> fixture.store(archive, SCHEMA));
      assertEquals(Files.readString(immutable), "corrupted");
      Path extra = fixture._directory.resolve("generated/segment/extra");
      Files.writeString(extra, "not covered by creation metadata");
      Path corruptArchive = fixture._directory.resolve("corrupt.tar.gz");
      TarCompressionUtils.createCompressedTarFile(extra.getParent().toFile(), corruptArchive.toFile());
      expectThrows(IllegalArgumentException.class, () -> fixture.store(Files.readAllBytes(corruptArchive), SCHEMA));
      fixture.noStaging();
    }
  }

  @Test
  public void testEscapedIncludesAndInterpolationAreRejectedBeforeMetadataEvaluation() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.archive();
      Path segment = fixture._directory.resolve("generated/segment");
      Path metadata;
      try (var files = Files.walk(segment)) {
        metadata = files.filter(path -> path.getFileName().toString().equals("metadata.properties"))
            .findFirst().orElseThrow();
      }
      byte[] original = Files.readAllBytes(metadata);
      for (String property : List.of("inc\\u006cude = file:///native-upload-must-not-read",
          "InClUdE\\u004fptional = file:///native-upload-must-not-read",
          "custom.value = \\u0024{sys:user.home}", "custom.\\u0024{sys:user.home} = literal")) {
        Files.write(metadata, original);
        Files.writeString(metadata, "\n" + property + "\n", StandardOpenOption.APPEND);
        Path archive = fixture._directory.resolve("properties.tar.gz");
        TarCompressionUtils.createCompressedTarFile(segment.toFile(), archive.toFile());
        IllegalArgumentException failure = expectThrows(IllegalArgumentException.class,
            () -> fixture.store(Files.readAllBytes(archive), SCHEMA));
        assertTrue(failure.getMessage().contains("Segment properties must be literal"));
      }
      Files.write(metadata, original);
      Path indexMap = segment.resolve("index_map");
      Files.writeString(indexMap, "inc\\u006cudeOptional = file:///native-upload-must-not-read");
      Path archive = fixture._directory.resolve("index-map.tar.gz");
      TarCompressionUtils.createCompressedTarFile(segment.toFile(), archive.toFile());
      assertTrue(expectThrows(IllegalArgumentException.class,
          () -> fixture.store(Files.readAllBytes(archive), SCHEMA)).getMessage().contains("must be literal"));
      fixture.noStaging();
    }
  }

  @Test
  public void testTraversalLinksDuplicatesAndExpansionBounds() throws Exception {
    try (Fixture fixture = new Fixture()) {
      for (String name : List.of("../outside", "segment/../../outside", "/outside", "segment\\outside")) {
        byte[] archive = tar(new TarArchiveEntry(name), new byte[0]);
        expectThrows(IllegalArgumentException.class, () -> fixture.store(archive, SCHEMA));
      }
      TarArchiveEntry link = new TarArchiveEntry("segment/link", TarConstants.LF_SYMLINK);
      link.setLinkName("/outside");
      expectThrows(IllegalArgumentException.class, () -> fixture.store(tar(link, new byte[0]), SCHEMA));
      byte[] duplicate = tar(new TarArchiveEntry("segment/duplicate"), new byte[0],
          new TarArchiveEntry("segment/duplicate"));
      expectThrows(IllegalArgumentException.class, () -> fixture.store(duplicate, SCHEMA));
      for (byte kind : List.of(TarConstants.LF_PAX_EXTENDED_HEADER_LC, TarConstants.LF_GNUTYPE_LONGNAME)) {
        byte[] oversizedHeader = extension(kind, 1024L * 1024 * 1024, new byte[0], 1);
        expectThrows(IllegalArgumentException.class, () -> fixture.store(oversizedHeader, SCHEMA));
      }
      byte[] paxLength = "2147483647 path=x\n".getBytes(StandardCharsets.US_ASCII);
      expectThrows(IllegalArgumentException.class, () -> fixture.store(
          extension(TarConstants.LF_PAX_EXTENDED_HEADER_LC, paxLength.length, paxLength, 1), SCHEMA));
      expectThrows(IllegalArgumentException.class, () -> fixture.store(
          extension(TarConstants.LF_GNUTYPE_LONGNAME, 2, new byte[]{'x', 0}, 5), SCHEMA));
      fixture._conf.setProperty(NativeSegmentUploadStorage.PREFIX + "maxExpandedBytes", 1024);
      byte[] bomb = tar(new TarArchiveEntry("segment/large"), new byte[16 * 1024]);
      expectThrows(IllegalArgumentException.class, () -> fixture.store(bomb, SCHEMA));
      fixture.noStaging();
    }
  }

  @Test
  public void testActualBodyLimitMultiplePartsAndSymlinkedDestination() throws Exception {
    try (Fixture fixture = new Fixture()) {
      byte[] archive = fixture.archive();
      fixture._conf.setProperty(NativeSegmentUploadStorage.PREFIX + "maxArchiveBytes", 32);
      expectThrows(IllegalArgumentException.class, () -> fixture.store(archive, SCHEMA));
      fixture._conf.setProperty(NativeSegmentUploadStorage.PREFIX + "maxArchiveBytes", 128L * 1024 * 1024);
      byte[] multipart = multipart(archive, true);
      expectThrows(IllegalArgumentException.class,
          () -> new NativeSegmentUploadStorage(fixture._conf).store(new ByteArrayInputStream(multipart), BOUNDARY,
              fixture._history, fixture._tableId, TABLE, SCHEMA));
      for (boolean second : List.of(false, true)) {
        byte[] header = oversizedMultipartHeader(archive, second);
        expectThrows(IllegalArgumentException.class,
            () -> new NativeSegmentUploadStorage(fixture._conf).store(new ByteArrayInputStream(header), BOUNDARY,
                fixture._history, fixture._tableId, TABLE, SCHEMA));
      }
      Path outside = Files.createDirectory(fixture._directory.resolve("outside"));
      Files.createSymbolicLink(fixture._root.resolve(fixture._history.toString()), outside);
      expectThrows(IllegalArgumentException.class, () -> fixture.store(archive, SCHEMA));
      try (var files = Files.list(outside)) {
        assertEquals(files.count(), 0L);
      }
      fixture.noStaging();
    }
  }

  @Test
  public void testOrdinaryGnuLongSegmentNamesRemainSupported() throws Exception {
    try (Fixture fixture = new Fixture()) {
      String name = "segment_" + "a".repeat(150);
      Content content = fixture.store(fixture.archive(name), SCHEMA);
      assertEquals(content.segmentName(), name);
      fixture.noStaging();
    }
  }

  private static byte[] extension(byte kind, long size, byte[] payload, int count) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (var gzip = new GzipCompressorOutputStream(output)) {
      for (int i = 0; i < count; i++) {
        TarArchiveEntry entry = new TarArchiveEntry("././@LongLink", kind);
        entry.setSize(size);
        byte[] header = new byte[512];
        entry.writeEntryHeader(header);
        gzip.write(header);
        gzip.write(payload);
        gzip.write(new byte[(512 - payload.length % 512) % 512]);
      }
      gzip.write(new byte[1024]);
    }
    return output.toByteArray();
  }

  private static byte[] oversizedMultipartHeader(byte[] archive, boolean second) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    output.write(("--" + BOUNDARY + "\r\nContent-Disposition: form-data; name=\"file\";"
        + " filename=\"file.tar.gz\"\r\n").getBytes(StandardCharsets.US_ASCII));
    if (second) {
      output.write("\r\n".getBytes(StandardCharsets.US_ASCII));
      output.write(archive);
      output.write(("\r\n--" + BOUNDARY + "\r\n").getBytes(StandardCharsets.US_ASCII));
    }
    output.write(("X-Oversized: " + "x".repeat(128 * 1024) + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
    output.write(archive);
    output.write(("\r\n--" + BOUNDARY + "--\r\n").getBytes(StandardCharsets.US_ASCII));
    return output.toByteArray();
  }

  private static byte[] tar(TarArchiveEntry first, byte[] bytes, TarArchiveEntry... more) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (var gzip = new GzipCompressorOutputStream(output); var tar = new TarArchiveOutputStream(gzip)) {
      first.setSize(bytes.length);
      tar.putArchiveEntry(first);
      tar.write(bytes);
      tar.closeArchiveEntry();
      for (TarArchiveEntry entry : more) {
        tar.putArchiveEntry(entry);
        tar.closeArchiveEntry();
      }
    }
    return output.toByteArray();
  }

  private static byte[] multipart(byte[] archive, boolean extra) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    output.write(("--" + BOUNDARY + "\r\nContent-Disposition: form-data; name=\"archive.tar.gz\";"
        + " filename=\"../../ignored.tar.gz\"\r\nContent-Type: application/octet-stream\r\n\r\n")
        .getBytes(StandardCharsets.US_ASCII));
    output.write(archive);
    if (extra) {
      output.write(("\r\n--" + BOUNDARY
          + "\r\nContent-Disposition: form-data; name=\"extra\"\r\n\r\nextra").getBytes(StandardCharsets.US_ASCII));
    }
    output.write(("\r\n--" + BOUNDARY + "--\r\n").getBytes(StandardCharsets.US_ASCII));
    return output.toByteArray();
  }

  private static final class Fixture implements AutoCloseable {
    private final Path _directory = Files.createTempDirectory("native-segment-upload-");
    private final Path _root = Files.createDirectory(_directory.resolve("shared"));
    private final ControllerConf _conf = new ControllerConf();
    private final UUID _history = UUID.randomUUID();
    private final UUID _tableId = UUID.randomUUID();

    private Fixture() throws Exception {
      _conf.setProperty(NativeSegmentUploadStorage.PREFIX + "storageRoot", _root.toString());
    }

    private byte[] archive() throws Exception {
      return archive("segment");
    }

    private byte[] archive(String segmentName) throws Exception {
      Path generated = Files.createDirectory(_directory.resolve("generated"));
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE, SCHEMA);
      config.setTableName("source");
      config.setOutDir(generated.toString());
      config.setSegmentName(segmentName);
      config.setCreationTime("123456789");
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      GenericRow row = new GenericRow();
      row.putValue("id", 1);
      driver.init(config, new GenericRowRecordReader(List.of(row)));
      driver.build();
      Path archive = _directory.resolve("segment.tar.gz");
      TarCompressionUtils.createCompressedTarFile(generated.resolve(segmentName).toFile(), archive.toFile());
      return Files.readAllBytes(archive);
    }

    private Content store(byte[] archive, Schema schema) throws Exception {
      return new NativeSegmentUploadStorage(_conf).store(new ByteArrayInputStream(multipart(archive, false)),
          BOUNDARY, _history, _tableId, TABLE, schema);
    }

    private void noStaging() throws Exception {
      try (var files = Files.list(_root)) {
        assertTrue(files.noneMatch(path -> path.getFileName().toString().startsWith(".upload-")));
      }
    }

    @Override
    public void close() throws Exception {
      FileUtils.deleteDirectory(_directory.toFile());
    }
  }

  /// Model a request entity whose container rejects reads after close, unlike ByteArrayInputStream.
  private static final class CloseAwareInput extends FilterInputStream {
    private boolean _closed;

    private CloseAwareInput(byte[] bytes) {
      super(new ByteArrayInputStream(bytes));
    }

    @Override
    public int read() throws IOException {
      checkOpen();
      return in.read();
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
      checkOpen();
      return in.read(bytes, offset, length);
    }

    @Override
    public void close() throws IOException {
      _closed = true;
      super.close();
    }

    private void checkOpen() throws IOException {
      if (_closed) {
        throw new IOException("Request entity is closed");
      }
    }
  }
}
