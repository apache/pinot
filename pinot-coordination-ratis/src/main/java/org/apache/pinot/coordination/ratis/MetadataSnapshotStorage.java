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
package org.apache.pinot.coordination.ratis;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;


/// Checksummed atomic snapshots. A checkpoint outside Ratis's replaceable snapshot directory prevents recovery from
/// silently treating lost compacted state as a new empty catalog. Methods run under the state machine's snapshot lock.
final class MetadataSnapshotStorage implements StateMachineStorage {
  private static final long MAGIC = 0x50494e4f54524131L;
  private static final int MAX_SNAPSHOT_BYTES = 256 * 1024 * 1024;
  private static final int HEADER_BYTES = Long.BYTES + Integer.BYTES + 32;
  private static final Pattern NAME = Pattern.compile("snapshot\\.(\\d+)_(\\d+)");
  private static final ObjectMapper READER = JsonMapper.builder(
          JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build();
  private static final Set<String> FIELDS = Set.of("formatVersion", "clusterId", "historyId", "term", "index", "state");
  private final UUID _clusterId;
  private final UUID _historyId;
  private Path _directory;
  private Path _checkpoint;
  private Path _installMarker;
  private volatile SingleFileSnapshotInfo _latest;

  MetadataSnapshotStorage(UUID clusterId, UUID historyId) {
    _clusterId = clusterId;
    _historyId = historyId;
  }

  @Override
  public synchronized void init(RaftStorage storage)
      throws IOException {
    _directory = storage.getStorageDir().getStateMachineDir().toPath();
    Path root = storage.getStorageDir().getRoot().toPath();
    _checkpoint = root.resolve("pinot-snapshot-checkpoint");
    _installMarker = root.resolve("pinot-snapshot-installing");
    createDirectoriesDurably(_directory);
    // Ratis creates current/ and the group directory without syncing their directory entries. Seal this hierarchy
    // during state-machine initialization, before the voter can acknowledge any metadata command.
    forceDirectory(root.resolve("current"));
    forceDirectory(root);
    forceDirectory(root.getParent());
  }

  synchronized SnapshotImage load()
      throws IOException {
    List<Path> files = snapshotFiles();
    long requiredIndex = readMarker(_checkpoint);
    long installingAfter = readMarker(_installMarker);
    if (files.isEmpty()) {
      if (requiredIndex >= 0 || Files.exists(_installMarker)) {
        throw new IOException("Required durable snapshot is missing; repair or restore voter storage: " + _directory);
      }
      _latest = null;
      return null;
    }
    Path file = files.get(0);
    TermIndex termIndex = termIndex(file);
    if (termIndex.getIndex() < requiredIndex
        || Files.exists(_installMarker) && termIndex.getIndex() <= installingAfter) {
      throw new IOException("Snapshot is older than the durable recovery checkpoint or interrupted installation: "
          + file);
    }
    long size = Files.size(file);
    if (size < HEADER_BYTES || size > (long) MAX_SNAPSHOT_BYTES + HEADER_BYTES) {
      throw new IOException("Invalid snapshot length: " + file);
    }
    byte[] payload;
    try (DataInputStream input = new DataInputStream(Files.newInputStream(file))) {
      if (input.readLong() != MAGIC) {
        throw new IOException("Unsupported snapshot format: " + file);
      }
      int length = input.readInt();
      if (length < 0 || length > MAX_SNAPSHOT_BYTES || size != (long) length + HEADER_BYTES) {
        throw new IOException("Invalid snapshot payload length: " + file);
      }
      byte[] expectedDigest = input.readNBytes(32);
      payload = input.readNBytes(length);
      if (payload.length != length || !MessageDigest.isEqual(expectedDigest, digest(payload))) {
        throw new IOException("Snapshot checksum mismatch: " + file);
      }
    }
    JsonNode envelope = READER.readTree(payload);
    if (envelope == null || !envelope.isObject() || envelope.size() != FIELDS.size()
        || !envelope.path("formatVersion").isIntegralNumber() || !envelope.path("formatVersion").canConvertToInt()
        || envelope.path("formatVersion").intValue() != 1
        || !_clusterId.toString().equals(envelope.path("clusterId").asText())
        || !_historyId.toString().equals(envelope.path("historyId").asText())
        || !envelope.path("term").isIntegralNumber() || !envelope.path("term").canConvertToLong()
        || !envelope.path("index").isIntegralNumber() || !envelope.path("index").canConvertToLong()
        || envelope.path("term").asLong() != termIndex.getTerm()
        || envelope.path("index").asLong() != termIndex.getIndex() || !envelope.has("state")) {
      throw new IOException("Snapshot identity, version or position does not match voter storage: " + file);
    }
    MD5Hash md5 = MD5FileUtil.computeMd5ForFile(file.toFile());
    MD5Hash storedMd5 = MD5FileUtil.readStoredMd5ForFile(file.toFile());
    if (storedMd5 != null && !storedMd5.equals(md5)) {
      throw new IOException("Snapshot transfer checksum mismatch: " + file);
    }
    // A crash between final-file rename and sidecar publication is recoverable because the file contains its SHA-256.
    if (storedMd5 == null) {
      MD5FileUtil.saveMD5File(file.toFile(), md5);
    }
    force(file);
    force(MD5FileUtil.getDigestFileForFile(file.toFile()).toPath());
    forceDirectory(_directory);
    forceDirectory(_directory.getParent());
    return new SnapshotImage(new SingleFileSnapshotInfo(new FileInfo(file, md5), termIndex), envelope.get("state"));
  }

  synchronized void publishLoaded(SnapshotImage image)
      throws IOException {
    // Called only after the catalog validates and restores every state record.
    checkpoint(image._info.getIndex());
    _latest = image._info;
    Files.deleteIfExists(_installMarker);
    forceDirectory(_installMarker.getParent());
  }

  synchronized long write(JsonNode state, TermIndex position)
      throws IOException {
    if (_latest != null && _latest.getIndex() >= position.getIndex()) {
      return _latest.getIndex();
    }
    ObjectNode envelope = JsonUtils.newObjectNode().put("formatVersion", 1).put("clusterId", _clusterId.toString())
        .put("historyId", _historyId.toString()).put("term", position.getTerm()).put("index", position.getIndex());
    envelope.set("state", state);
    byte[] payload = JsonUtils.objectToBytes(envelope);
    if (payload.length > MAX_SNAPSHOT_BYTES) {
      throw new IOException("Metadata snapshot exceeds the initial 256 MiB snapshot limit");
    }
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(HEADER_BYTES + payload.length);
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeLong(MAGIC);
      output.writeInt(payload.length);
      output.write(digest(payload));
      output.write(payload);
    }
    Path file = _directory.resolve("snapshot." + position.getTerm() + "_" + position.getIndex());
    atomicWrite(file, bytes.toByteArray());
    MD5Hash md5 = MD5FileUtil.computeMd5ForFile(file.toFile());
    MD5FileUtil.saveMD5File(file.toFile(), md5);
    force(MD5FileUtil.getDigestFileForFile(file.toFile()).toPath());
    forceDirectory(_directory);
    checkpoint(position.getIndex());
    _latest = new SingleFileSnapshotInfo(new FileInfo(file, md5), position);
    return position.getIndex();
  }

  synchronized void beginInstall(long appliedIndex)
      throws IOException {
    atomicWrite(_installMarker, JsonUtils.objectToBytes(appliedIndex));
  }

  private void checkpoint(long index)
      throws IOException {
    atomicWrite(_checkpoint, JsonUtils.objectToBytes(index));
  }

  private long readMarker(Path path)
      throws IOException {
    if (!Files.exists(path)) {
      return -1;
    }
    if (Files.size(path) > 128) {
      throw new IOException("Invalid snapshot recovery marker length: " + path);
    }
    JsonNode node = readJson(Files.readAllBytes(path));
    long minimum = path.equals(_checkpoint) ? 0 : -1;
    if (node == null || !node.isIntegralNumber() || !node.canConvertToLong() || node.longValue() < minimum) {
      throw new IOException("Invalid snapshot recovery marker: " + path);
    }
    return node.longValue();
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    return _latest;
  }

  @Override
  public void format()
      throws IOException {
    throw new IOException("Automatic snapshot storage formatting is prohibited");
  }

  @Override
  public synchronized void cleanupOldSnapshots(SnapshotRetentionPolicy policy)
      throws IOException {
    // Keep the previous complete snapshot even if the caller supplies a smaller policy.
    int retain = Math.max(2, policy.getNumSnapshotsRetained());
    List<Path> files = snapshotFiles();
    for (int i = retain; i < files.size(); i++) {
      Path file = files.get(i);
      Files.delete(file);
      Files.deleteIfExists(MD5FileUtil.getDigestFileForFile(file.toFile()).toPath());
    }
    forceDirectory(_directory);
  }

  private List<Path> snapshotFiles()
      throws IOException {
    try (Stream<Path> paths = Files.list(_directory)) {
      return paths.filter(path -> NAME.matcher(path.getFileName().toString()).matches())
          .peek(MetadataSnapshotStorage::termIndex)
          .sorted(Comparator.comparingLong((Path path) -> termIndex(path).getIndex()).reversed()).toList();
    } catch (NumberFormatException e) {
      throw new IOException("Invalid snapshot position in " + _directory, e);
    }
  }

  private static TermIndex termIndex(Path path) {
    Matcher matcher = NAME.matcher(path.getFileName().toString());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid snapshot name: " + path);
    }
    return TermIndex.valueOf(Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)));
  }

  private static byte[] digest(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by the JVM", e);
    }
  }

  static JsonNode readJson(byte[] bytes)
      throws IOException {
    return READER.readTree(bytes);
  }

  static void createDirectoriesDurably(Path directory)
      throws IOException {
    Path existing = directory;
    while (existing != null && !Files.exists(existing)) {
      existing = existing.getParent();
    }
    Files.createDirectories(directory);
    for (Path current = directory; current != null; current = current.getParent()) {
      forceDirectory(current);
      if (current.equals(existing)) {
        break;
      }
    }
  }

  static void atomicWrite(Path target, byte[] bytes)
      throws IOException {
    Path temporary = target.resolveSibling(target.getFileName() + "." + UUID.randomUUID() + ".tmp");
    try {
      try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
          output.write(buffer);
        }
        output.force(true);
      }
      Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      forceDirectory(target.getParent());
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void force(Path file)
      throws IOException {
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
      channel.force(true);
    }
  }

  private static void forceDirectory(Path directory)
      throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  /// Detached, validated disk image; publication follows catalog validation.
  static final class SnapshotImage {
    final SingleFileSnapshotInfo _info;
    final JsonNode _state;

    SnapshotImage(SingleFileSnapshotInfo info, JsonNode state) {
      _info = info;
      _state = state;
    }
  }
}
