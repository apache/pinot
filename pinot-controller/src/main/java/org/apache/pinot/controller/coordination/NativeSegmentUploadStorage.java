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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.upload.SegmentValidationUtils;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.segment.local.utils.CrcUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.jvnet.mimepull.MIMEConfig;
import org.jvnet.mimepull.MIMEMessage;
import org.jvnet.mimepull.MIMEParsingException;
import org.jvnet.mimepull.MIMEPart;

import static com.google.common.base.Preconditions.checkArgument;


/// Validates bounded uploads and durably links immutable archives into an explicitly configured shared filesystem.
/// Each invocation owns its staging directory. Final objects are never overwritten or deleted on an uncertain commit.
final class NativeSegmentUploadStorage {
  static final String PREFIX = NativeMetadataConfig.PREFIX + "controller.upload.";
  private static final int MULTIPART_OVERHEAD_BYTES = 64 * 1024;
  @Nullable
  private final Path _root;
  private final long _maxArchiveBytes;
  private final long _maxExpandedBytes;
  private final int _maxEntries;

  NativeSegmentUploadStorage(ControllerConf conf) throws IOException {
    _maxArchiveBytes = conf.getProperty(PREFIX + "maxArchiveBytes", 128L * 1024 * 1024);
    _maxExpandedBytes = conf.getProperty(PREFIX + "maxExpandedBytes", 512L * 1024 * 1024);
    _maxEntries = conf.getProperty(PREFIX + "maxEntries", 4096);
    checkArgument(_maxArchiveBytes > 0 && _maxArchiveBytes <= 512L * 1024 * 1024
        && _maxExpandedBytes > 0 && _maxExpandedBytes <= 2L * 1024 * 1024 * 1024
        && _maxEntries > 0 && _maxEntries <= 16384, "Native upload limits exceed supported bounds");
    String root = conf.getProperty(PREFIX + "storageRoot");
    if (root == null || root.isBlank()) {
      _root = null;
    } else {
      Path path = Path.of(root);
      checkArgument(path.isAbsolute() && path.equals(path.normalize()), "Upload root must be absolute and normalized");
      _root = path.toRealPath();
      verifyDirectories(_root);
    }
  }

  boolean enabled() {
    return _root != null;
  }

  long maxRequestBytes() {
    return _maxArchiveBytes + MULTIPART_OVERHEAD_BYTES;
  }

  Content store(InputStream entity, String boundary, UUID history, UUID tableId, TableConfig table, Schema schema)
      throws IOException {
    checkArgument(_root != null, "Native upload storage is not configured");
    checkArgument(boundary != null && !boundary.isBlank() && boundary.length() <= 200
        && boundary.chars().allMatch(c -> c >= 32 && c < 127), "Invalid multipart boundary");
    verifyDirectories(_root);
    Path temporary = Files.createTempDirectory(_root, ".upload-");
    try {
      Path archive = temporary.resolve("archive.tar.gz");
      String digest = receive(entity, boundary, temporary, archive);
      Path extracted = Files.createDirectory(temporary.resolve("extracted"));
      Path segment = extract(archive, extracted);
      SegmentMetadataImpl metadata;
      try {
        metadata = new SegmentMetadataImpl(segment.toFile());
        validate(metadata, segment, table, schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid Pinot segment metadata", e);
      } catch (ConfigurationException e) {
        throw new IllegalArgumentException("Invalid Pinot segment metadata", e);
      }
      Path directory = _root.resolve(history.toString()).resolve(tableId.toString());
      createDirectories(directory);
      Path destination = directory.resolve(digest + ".tar.gz");
      try {
        // Linking a fully fsynced inode is atomic and fails if another controller already published this digest.
        Files.createLink(destination, archive);
      } catch (FileAlreadyExistsException e) {
        checkArgument(Files.isRegularFile(destination, LinkOption.NOFOLLOW_LINKS)
            && Files.size(destination) == Files.size(archive) && digest.equals(digest(destination)),
            "Existing immutable upload artifact does not match its digest");
      }
      for (Path parent = directory; parent.startsWith(_root); parent = parent.getParent()) {
        try (FileChannel channel = FileChannel.open(parent, StandardOpenOption.READ)) {
          channel.force(true);
        }
      }
      UUID objectId = UUID.nameUUIDFromBytes((history + "\0NATIVE_UPLOAD\0" + tableId + "\0"
          + metadata.getName() + "\0" + digest).getBytes(StandardCharsets.UTF_8));
      // Data CRC predates v3 index packing. Use the verified full-segment CRC, not an unverified data-CRC claim.
      return new Content(table.getTableName(), tableId, metadata.getName(), objectId, digest,
          destination.toUri().toString(), new LoaderDescriptor(metadata.getCrc(), -1, false, null, null,
          metadata.getIndexCreationTime(), -1, Map.of()));
    } finally {
      FileUtils.deleteDirectory(temporary.toFile());
    }
  }

  private String receive(InputStream entity, String boundary, Path temporary, Path archive) throws IOException {
    MIMEConfig config = new MIMEConfig();
    config.setMemoryThreshold(8192);
    config.setDir(temporary.toString());
    MessageDigest digest = sha256();
    try (LimitedInputStream bounded = new LimitedInputStream(entity, MULTIPART_OVERHEAD_BYTES);
        MIMEMessage message = new MIMEMessage(new FilterInputStream(bounded) {
          @Override
          public void close() {
            // MIMEPull closes on END_MESSAGE. This method owns the entity until its bounded epilogue is consumed.
          }
        }, boundary, config)) {
      MIMEPart part = message.getPart(0);
      List<String> dispositions = part.getHeader("Content-Disposition");
      checkArgument(dispositions != null && dispositions.size() == 1
          && dispositions.getFirst().length() <= 8192, "Upload requires one file part");
      FormDataContentDisposition disposition = new FormDataContentDisposition(dispositions.getFirst());
      checkArgument("form-data".equalsIgnoreCase(disposition.getType()) && disposition.getName() != null
          && disposition.getFileName() != null, "Upload requires one file part");
      String encoding = part.getContentTransferEncoding();
      checkArgument(encoding == null || "binary".equalsIgnoreCase(encoding) || "8bit".equalsIgnoreCase(encoding)
          || "7bit".equalsIgnoreCase(encoding), "Encoded multipart parts are unsupported");
      bounded.limit(maxRequestBytes());
      long archiveBytes;
      try (InputStream input = new LimitedInputStream(part.readOnce(), _maxArchiveBytes);
          FileChannel output = FileChannel.open(archive, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
        byte[] buffer = new byte[8192];
        for (int count; (count = input.read(buffer)) != -1;) {
          digest.update(buffer, 0, count);
          ByteBuffer bytes = ByteBuffer.wrap(buffer, 0, count);
          while (bytes.hasRemaining()) {
            output.write(bytes);
          }
        }
        output.force(true);
        archiveBytes = output.size();
      }
      bounded.limit(archiveBytes + MULTIPART_OVERHEAD_BYTES);
      checkArgument(message.getAttachments().size() == 1, "Upload requires exactly one file part");
      byte[] buffer = new byte[8192];
      while (bounded.read(buffer) != -1) {
        // Bound any MIME epilogue as well as the file.
      }
    } catch (MIMEParsingException e) {
      throw new IllegalArgumentException("Invalid or oversized multipart upload", e);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Invalid multipart file disposition", e);
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  private Path extract(Path archive, Path extracted) throws IOException {
    Set<Path> entries = new HashSet<>();
    String root = null;
    int count = 0;
    long bytes = 0;
    // Also bound decompressor output consumed internally while parsing tar headers, including PAX headers.
    long tarLimit = _maxExpandedBytes + (long) _maxEntries * 1024 + MULTIPART_OVERHEAD_BYTES;
    try (InputStream file = Files.newInputStream(archive);
        InputStream gzip = GzipCompressorInputStream.builder().setInputStream(file)
            .setDecompressConcatenated(true).get();
        InputStream bounded = new LimitedInputStream(gzip, tarLimit);
        TarArchiveInputStream tar = new BoundedTarInputStream(bounded, _maxEntries)) {
      for (TarArchiveEntry entry; (entry = tar.getNextEntry()) != null;) {
        checkArgument(++count <= _maxEntries && (entry.isDirectory() || entry.isFile()) && !entry.isSparse()
            && !entry.isSymbolicLink() && !entry.isLink() && tar.canReadEntryData(entry),
            "Unsupported archive entry or excessive entry count");
        String name = entry.getName();
        checkArgument(name != null && !name.isBlank() && name.length() <= 4096 && !name.contains("\\")
            && !name.startsWith("/") && name.chars().noneMatch(c -> c < 32), "Invalid archive entry path");
        Path relative = Path.of(name);
        checkArgument(!relative.isAbsolute() && relative.normalize().equals(relative)
            && !relative.startsWith("..") && relative.getNameCount() <= 16, "Archive path escapes its segment");
        if (root == null) {
          root = relative.getName(0).toString();
        }
        checkArgument(relative.getName(0).toString().equals(root) && entries.add(relative),
            "Archive requires one segment directory without duplicate entries");
        Path destination = extracted.resolve(relative);
        checkArgument(destination.startsWith(extracted) && (relative.getNameCount() > 1 || entry.isDirectory()),
            "Archive requires a segment directory");
        if (entry.isDirectory()) {
          Files.createDirectories(destination);
        } else {
          long size = entry.getSize();
          checkArgument(size >= 0 && size <= _maxExpandedBytes - bytes, "Expanded archive exceeds its byte limit");
          String fileName = relative.getFileName().toString();
          if (fileName.equals(V1Constants.MetadataKeys.METADATA_FILE_NAME)
              || fileName.equals(V1Constants.INDEX_MAP_FILE_NAME)) {
            checkArgument(size <= 2 * 1024 * 1024, "Segment metadata exceeds its byte limit");
          }
          if (fileName.equals(V1Constants.SEGMENT_CREATION_META)) {
            checkArgument(size == 16 || size == 24, "Invalid segment creation metadata");
          }
          bytes += size;
          Files.createDirectories(destination.getParent());
          try (var output = Files.newOutputStream(destination, StandardOpenOption.CREATE_NEW)) {
            tar.transferTo(output);
          }
          checkArgument(Files.size(destination) == size, "Truncated archive entry");
          if (fileName.equals(V1Constants.MetadataKeys.METADATA_FILE_NAME)
              || fileName.equals(V1Constants.INDEX_MAP_FILE_NAME)) {
            validateProperties(destination);
          }
        }
      }
      // Reach the gzip trailer so its own checksum is verified; only tar padding may remain.
      byte[] padding = new byte[8192];
      for (int size; (size = bounded.read(padding)) != -1;) {
        for (int i = 0; i < size; i++) {
          checkArgument(padding[i] == 0, "Archive contains trailing non-padding content");
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid or oversized segment archive", e);
    }
    checkArgument(root != null, "Empty segment archive");
    return extracted.resolve(root);
  }

  private static void validateProperties(Path file) throws IOException {
    // Decode escapes and continuations exactly as the default segment reader, without loading a configuration:
    // includes and interpolation must be rejected before SegmentMetadataImpl can evaluate either mechanism.
    try (var reader = new PropertiesConfiguration.PropertiesReader(new InputStreamReader(Files.newInputStream(file)))) {
      while (reader.nextProperty()) {
        String key = reader.getPropertyName();
        String value = reader.getPropertyValue();
        checkArgument(!key.equalsIgnoreCase("include") && !key.equalsIgnoreCase("includeOptional")
            && !key.equalsIgnoreCase(PropertiesConfiguration.getInclude())
            && !key.equalsIgnoreCase(PropertiesConfiguration.getIncludeOptional())
            && !key.contains("${") && !value.contains("${"), "Segment properties must be literal without includes");
      }
    } catch (ConfigurationRuntimeException e) {
      throw new IllegalArgumentException("Invalid segment properties", e);
    }
  }

  private static void validate(SegmentMetadataImpl metadata, Path segment, TableConfig table, Schema schema)
      throws IOException {
    String name = metadata.getName();
    checkArgument(name != null && !name.isBlank() && name.length() <= 1024 && !name.contains("/")
        && !name.contains("\\") && !name.equals(".") && !name.equals("..")
        && name.chars().noneMatch(c -> c < 32) && LLCSegmentName.of(name) == null,
        "Invalid OFFLINE segment name");
    checkArgument(metadata.getTotalDocs() >= 0 && metadata.getCrc() >= 0 && metadata.getIndexCreationTime() >= -1,
        "Segment requires complete creation metadata");
    checkArgument(metadata.getCrc() == CrcUtils.computeCrc(segment.toFile()), "Segment CRC does not match its bytes");
    Schema actual = metadata.getSchema();
    checkArgument(actual.getPhysicalColumnNames().equals(schema.getPhysicalColumnNames()),
        "Segment columns differ from the destination schema");
    for (String column : schema.getPhysicalColumnNames()) {
      FieldSpec expected = schema.getFieldSpecFor(column);
      FieldSpec observed = actual.getFieldSpecFor(column);
      checkArgument(expected.getDataType() == observed.getDataType()
          && expected.isSingleValueField() == observed.isSingleValueField()
          && expected.getFieldType() == observed.getFieldType(), "Segment column differs from destination schema");
    }
    if (table.getIngestionConfig() == null || table.getIngestionConfig().isSegmentTimeValueCheck()) {
      SegmentValidationUtils.validateTimeInterval(metadata, table);
    }
  }

  private void createDirectories(Path directory) throws IOException {
    Path current = _root;
    for (Path component : _root.relativize(directory)) {
      current = current.resolve(component);
      try {
        Files.createDirectory(current);
      } catch (FileAlreadyExistsException e) {
        checkArgument(Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS), "Upload path is not a directory");
      }
    }
    verifyDirectories(directory);
  }

  private static void verifyDirectories(Path path) throws IOException {
    for (Path current = path; current != null; current = current.getParent()) {
      checkArgument(Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS),
          "Upload root must exist without symbolic links");
    }
  }

  private String digest(Path file) throws IOException {
    MessageDigest digest = sha256();
    try (InputStream input = new DigestInputStream(
        new LimitedInputStream(Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS), _maxArchiveBytes), digest)) {
      byte[] buffer = new byte[8192];
      while (input.read(buffer) != -1) {
        // Validate a competing writer's complete immutable inode before referring to it.
      }
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  /// Commons parses extension entries internally before returning a file. Check their headers and bounded PAX
  /// framing first, preventing oversized allocations, recursive GNU header chains and sparse-map parsing.
  private static final class BoundedTarInputStream extends TarArchiveInputStream {
    private static final int EXTENSION_LIMIT = 64 * 1024;
    private final PushbackInputStream _source;
    private final int _headerLimit;
    private int _headers;
    private int _consecutiveExtensions;
    private long _extensionBytes;
    private boolean _sawEof;

    private BoundedTarInputStream(InputStream source, int maxEntries) {
      this(new PushbackInputStream(source, EXTENSION_LIMIT), maxEntries);
    }

    private BoundedTarInputStream(PushbackInputStream source, int maxEntries) {
      // Do not let Commons silently skip unvalidated 10-KiB end padding.
      super(source, 512);
      _source = source;
      _headerLimit = maxEntries * 5;
    }

    @Override
    protected byte[] readRecord() throws IOException {
      byte[] record = super.readRecord();
      if (record == null || isEOFRecord(record)) {
        _sawEof = true;
        return record;
      }
      checkArgument(!_sawEof && ++_headers <= _headerLimit, "Archive contains excessive or trailing headers");
      TarArchiveEntry header = new TarArchiveEntry(record);
      checkArgument(header.isCheckSumOK() && !header.isSparse() && !header.isSymbolicLink() && !header.isLink(),
          "Invalid archive header or unsupported links/sparse content");
      boolean pax = header.isPaxHeader() || header.isGlobalPaxHeader();
      boolean extended = pax || header.isGNULongNameEntry() || header.isGNULongLinkEntry();
      if (!extended) {
        _consecutiveExtensions = 0;
        return record;
      }
      long size = header.getSize();
      checkArgument(size >= 0 && size <= EXTENSION_LIMIT - _extensionBytes
          && (pax || size <= 16 * 1024) && ++_consecutiveExtensions <= 4,
          "Archive extension metadata exceeds its bounds");
      _extensionBytes += size;
      if (pax) {
        byte[] metadata = _source.readNBytes((int) size);
        checkArgument(metadata.length == size, "Truncated archive extension metadata");
        validatePax(metadata);
        _source.unread(metadata);
      }
      return record;
    }

    private static void validatePax(byte[] metadata) {
      int offset = 0;
      while (offset < metadata.length) {
        int space = offset;
        while (space < metadata.length && metadata[space] != ' ' && space - offset < 10) {
          checkArgument(metadata[space] >= '0' && metadata[space] <= '9', "Invalid PAX record length");
          space++;
        }
        checkArgument(space > offset && space < metadata.length && metadata[space] == ' ',
            "Invalid PAX record length");
        int length = Integer.parseInt(new String(metadata, offset, space - offset, StandardCharsets.US_ASCII));
        checkArgument(length > space - offset + 2 && length <= metadata.length - offset
            && metadata[offset + length - 1] == '\n', "Invalid PAX record bounds");
        String value = new String(metadata, space + 1, offset + length - space - 2, StandardCharsets.UTF_8);
        int equals = value.indexOf('=');
        checkArgument(equals > 0 && equals <= 128, "Invalid PAX metadata key");
        String key = value.substring(0, equals);
        checkArgument(!key.startsWith("GNU.sparse") && !key.equals("SCHILY.realsize")
            && !key.equals("SCHILY.filetype"), "Sparse PAX archives are unsupported");
        offset += length;
      }
    }
  }

  /// Counts actual reads, including chunked requests and decompressor-internal reads.
  private static final class LimitedInputStream extends FilterInputStream {
    private long _limit;
    private long _read;

    private LimitedInputStream(InputStream input, long limit) {
      super(input);
      _limit = limit;
    }

    private void limit(long limit) {
      checkArgument(_read <= limit, "Multipart overhead exceeds its byte limit");
      _limit = limit;
    }

    @Override
    public int read() throws IOException {
      int value = in.read();
      if (value != -1) {
        add(1);
      }
      return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
      int count = in.read(bytes, offset, (int) Math.min(length, _limit - _read + 1));
      if (count > 0) {
        add(count);
      }
      return count;
    }

    @Override
    public long skip(long amount) throws IOException {
      long skipped = 0;
      byte[] buffer = new byte[(int) Math.min(8192, Math.max(0, amount))];
      while (skipped < amount) {
        int count = read(buffer, 0, (int) Math.min(buffer.length, amount - skipped));
        if (count == -1) {
          break;
        }
        skipped += count;
      }
      return skipped;
    }

    private void add(int count) {
      _read += count;
      checkArgument(_read <= _limit, "Upload exceeds its byte limit");
    }
  }
}
