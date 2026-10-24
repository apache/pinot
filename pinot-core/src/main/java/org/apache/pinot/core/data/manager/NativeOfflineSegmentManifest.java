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
package org.apache.pinot.core.data.manager;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Installation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Mode;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Operation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Owner;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Request;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.spi.utils.JsonUtils;


/// Checksummed local selection of immutable installation files. Publication uses an atomic rename plus directory
/// fsync; the selected directory is never renamed, overwritten or removed while readers can hold it.
final class NativeOfflineSegmentManifest {
  private static final int MAX_FILES = 16384;
  private static final int MAX_MANIFEST_BYTES = 8 * 1024 * 1024;
  private static final ObjectMapper READER = new ObjectMapper()
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  private final ObjectNode _payload;
  private final Identity _identity;
  private final UUID _installationId;
  private final Mode _mode;
  private final boolean _forceDownload;
  private final byte[] _bytes;
  private final String _manifestSha256;

  private NativeOfflineSegmentManifest(ObjectNode payload, byte[] bytes) throws IOException {
    fields(payload, "format", "identity", "installationId", "mode", "forceDownload", "files");
    long format = number(payload, "format");
    require(format == 1 || format == 2, "Unsupported native installation manifest format");
    _identity = readIdentity(payload.get("identity"));
    _installationId = uuid(payload, "installationId");
    _mode = Mode.valueOf(string(payload, "mode"));
    require(format == 2 || _mode != Mode.RECONCILE, "Reconciliation requires native manifest format 2");
    require(payload.get("forceDownload").isBoolean(), "Malformed forceDownload");
    _forceDownload = payload.get("forceDownload").booleanValue();
    require(payload.get("files").isObject() && payload.get("files").size() > 0
        && payload.get("files").size() <= MAX_FILES, "Invalid installation file inventory");
    _payload = payload;
    _bytes = bytes;
    _manifestSha256 = digest(bytes);
  }

  static NativeOfflineSegmentManifest prepare(Request request, UUID installationId, Path segmentDirectory)
      throws IOException {
    ObjectNode payload = JsonUtils.newObjectNode();
    payload.put("format", request.mode() == Mode.RECONCILE ? 2 : 1);
    payload.set("identity", identity(request.identity()));
    payload.put("installationId", installationId.toString());
    payload.put("mode", request.mode().name());
    payload.put("forceDownload", request.forceDownload());
    ObjectNode files = payload.putObject("files");
    for (Map.Entry<String, Path> entry : inventory(segmentDirectory).entrySet()) {
      ObjectNode value = files.putObject(entry.getKey());
      value.put("size", Files.size(entry.getValue()));
      value.put("sha256", digest(entry.getValue()));
      forceFile(entry.getValue());
    }
    // Children must be durable before their parent entries and before the selection manifest can be published.
    try (Stream<Path> paths = Files.walk(segmentDirectory)) {
      for (Path path : paths.filter(Files::isDirectory).sorted(Comparator.reverseOrder())
          .collect(Collectors.toList())) {
        forceDirectory(path);
      }
    }
    byte[] bytes = envelope(payload);
    return new NativeOfflineSegmentManifest(payload, bytes);
  }

  static NativeOfflineSegmentManifest read(Path path) throws IOException {
    require(Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) && Files.size(path) <= MAX_MANIFEST_BYTES,
        "Missing or oversized native installation manifest");
    byte[] bytes = Files.readAllBytes(path);
    try {
      JsonNode envelope = READER.readTree(bytes);
      fields(envelope, "payload", "checksum");
      JsonNode payload = envelope.get("payload");
      require(payload.isObject() && digest(JsonUtils.objectToBytes(payload)).equals(string(envelope, "checksum")),
          "Native installation manifest checksum differs");
      return new NativeOfflineSegmentManifest((ObjectNode) payload, bytes);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new IOException("Malformed native installation manifest", e);
    }
  }

  void verifyFiles(Path segmentDirectory) throws IOException {
    // A loader can discover any file in this tree. Verify the complete inventory, not merely the files listed in
    // the manifest, so added indexes, symbolic links and unsupported file types cannot affect a recovered segment.
    Map<String, Path> actual = inventory(segmentDirectory);
    require(actual.size() == _payload.get("files").size(), "Installed file inventory differs");
    for (Map.Entry<String, JsonNode> entry : _payload.get("files").properties()) {
      require(actual.containsKey(entry.getKey()), "Installed file inventory differs");
      Path relative = Path.of(entry.getKey());
      require(!relative.isAbsolute() && relative.normalize().equals(relative)
          && !relative.startsWith("..") && relative.getNameCount() > 0, "Invalid installation file path");
      Path path = segmentDirectory.resolve(relative);
      for (Path component = path; component != null && !component.equals(segmentDirectory.getParent());
          component = component.getParent()) {
        require(!Files.isSymbolicLink(component), "Symbolic link in installation path");
      }
      fields(entry.getValue(), "size", "sha256");
      long size = number(entry.getValue(), "size");
      require(size >= 0 && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) && Files.size(path) == size
          && digest(path).equals(string(entry.getValue(), "sha256")), "Installed file is missing or corrupt");
    }
  }

  void copyVerifiedFiles(Path source, Path target) throws IOException {
    verifyFiles(source);
    createDirectories(target);
    for (Map.Entry<String, JsonNode> entry : _payload.get("files").properties()) {
      Path relative = Path.of(entry.getKey());
      Path destination = target.resolve(relative);
      createDirectories(destination.getParent());
      Files.copy(source.resolve(relative), destination, LinkOption.NOFOLLOW_LINKS);
    }
    // Verify again after copying: a changed source cannot be certified by the new installation's manifest.
    verifyFiles(target);
  }

  private static Map<String, Path> inventory(Path segmentDirectory) throws IOException {
    require(Files.isDirectory(segmentDirectory, LinkOption.NOFOLLOW_LINKS), "Missing installation directory");
    Map<String, Path> inventory = new TreeMap<>();
    try (Stream<Path> paths = Files.walk(segmentDirectory)) {
      Iterator<Path> iterator = paths.iterator();
      while (iterator.hasNext()) {
        Path path = iterator.next();
        require(!Files.isSymbolicLink(path), "Symbolic links cannot be installed");
        if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
          require(inventory.size() < MAX_FILES, "Too many installation files");
          inventory.put(segmentDirectory.relativize(path).toString(), path);
        } else {
          require(Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS), "Unsupported installation file type");
        }
      }
    }
    return inventory;
  }

  boolean matches(Request request) {
    return _identity.equals(request.identity()) && _mode == request.mode() && _forceDownload == request.forceDownload();
  }

  Identity identity() {
    return _identity;
  }

  UUID installationId() {
    return _installationId;
  }

  String manifestSha256() {
    return _manifestSha256;
  }

  Installation installation() {
    return new Installation(_identity, _installationId, _manifestSha256);
  }

  Path installationDirectory(Path root) {
    return root.resolve("materializations").resolve(_identity.historyId().toString())
        .resolve(_identity.table().objectId().toString()).resolve(_identity.content().objectId().toString())
        .resolve(_installationId.toString());
  }

  void write(Path path) throws IOException {
    atomicWrite(path, _bytes);
  }

  private static byte[] envelope(ObjectNode payload) throws IOException {
    ObjectNode envelope = JsonUtils.newObjectNode();
    envelope.set("payload", payload);
    envelope.put("checksum", digest(JsonUtils.objectToBytes(payload)));
    byte[] bytes = JsonUtils.objectToBytes(envelope);
    require(bytes.length <= MAX_MANIFEST_BYTES, "Native installation manifest is too large");
    return bytes;
  }

  static String digest(Path path) throws IOException {
    MessageDigest digest = sha256();
    try (InputStream input = Files.newInputStream(path)) {
      byte[] buffer = new byte[64 * 1024];
      int count;
      while ((count = input.read(buffer)) != -1) {
        digest.update(buffer, 0, count);
      }
    }
    return hex(digest.digest());
  }

  static String digest(byte[] bytes) {
    return hex(sha256().digest(bytes));
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String hex(byte[] bytes) {
    StringBuilder result = new StringBuilder(bytes.length * 2);
    for (byte value : bytes) {
      result.append(Character.forDigit((value >>> 4) & 15, 16)).append(Character.forDigit(value & 15, 16));
    }
    return result.toString();
  }

  static void createDirectories(Path directory) throws IOException {
    if (Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
      require(Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS), "Installation directory is not a directory");
      return;
    }
    Path parent = directory.getParent();
    if (parent != null) {
      createDirectories(parent);
    }
    try {
      Files.createDirectory(directory);
    } catch (FileAlreadyExistsException e) {
      require(Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS), "Installation path is not a directory");
    }
    forceDirectory(directory);
    if (parent != null) {
      forceDirectory(parent);
    }
  }

  // Table initialization uses the legacy mkdirs path. Its pre-existing directory chain may still contain newly
  // created entries, so make every ancestor durable before acknowledging the first native selection.
  static void forceAncestors(Path directory) throws IOException {
    for (Path current = directory.toAbsolutePath(); current != null; current = current.getParent()) {
      forceDirectory(current);
    }
  }

  private static void atomicWrite(Path target, byte[] bytes) throws IOException {
    createDirectories(target.getParent());
    Path temporary = target.resolveSibling(target.getFileName() + "." + UUID.randomUUID() + ".tmp");
    try {
      Files.write(temporary, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
      forceFile(temporary);
      Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      forceDirectory(target.getParent());
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void forceFile(Path file) throws IOException {
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
      channel.force(true);
    }
  }

  private static void forceDirectory(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  private static ObjectNode identity(Identity value) {
    ObjectNode node = JsonUtils.newObjectNode();
    node.put("historyId", value.historyId().toString());
    node.put("tableName", value.tableName());
    node.set("table", version(value.table()));
    node.put("schemaName", value.schemaName());
    node.set("schema", version(value.schema()));
    node.put("segmentName", value.segmentName());
    node.set("content", version(value.content()));
    node.put("contentSha256", value.contentSha256());
    node.set("server", participant(value.server()));
    node.set("assignment", version(value.assignment()));
    node.put("materializationId", value.materializationId().toString());
    ObjectNode operation = node.putObject("operation");
    operation.put("operationId", value.operation().operationId().toString());
    operation.put("attempt", value.operation().attempt());
    Owner owner = value.operation().owner();
    ObjectNode ownerNode = operation.putObject("owner");
    ownerNode.put("layoutGeneration", owner.layoutGeneration());
    ownerNode.put("tableShard", owner.tableShard());
    ownerNode.put("ownerEpoch", owner.ownerEpoch());
    ownerNode.set("controller", participant(owner.controller()));
    return node;
  }

  private static Identity readIdentity(JsonNode node) throws IOException {
    fields(node, "historyId", "tableName", "table", "schemaName", "schema", "segmentName", "content",
        "contentSha256", "server", "assignment", "materializationId", "operation");
    JsonNode operation = node.get("operation");
    fields(operation, "operationId", "attempt", "owner");
    JsonNode owner = operation.get("owner");
    fields(owner, "layoutGeneration", "tableShard", "ownerEpoch", "controller");
    long shard = number(owner, "tableShard");
    require(shard >= 0 && shard <= Integer.MAX_VALUE, "Invalid table shard");
    return new Identity(uuid(node, "historyId"), string(node, "tableName"), readVersion(node.get("table")),
        string(node, "schemaName"), readVersion(node.get("schema")), string(node, "segmentName"),
        readVersion(node.get("content")), string(node, "contentSha256"), readParticipant(node.get("server")),
        readVersion(node.get("assignment")), uuid(node, "materializationId"),
        new Operation(uuid(operation, "operationId"), number(operation, "attempt"),
            new Owner(number(owner, "layoutGeneration"), (int) shard, number(owner, "ownerEpoch"),
                readParticipant(owner.get("controller")))));
  }

  private static ObjectNode version(ObjectVersion value) {
    ObjectNode node = JsonUtils.newObjectNode();
    node.put("objectId", value.objectId().toString());
    node.put("version", value.version());
    return node;
  }

  private static ObjectVersion readVersion(JsonNode node) throws IOException {
    fields(node, "objectId", "version");
    return new ObjectVersion(uuid(node, "objectId"), number(node, "version"));
  }

  private static ObjectNode participant(Participant value) {
    ObjectNode node = JsonUtils.newObjectNode();
    node.put("participantId", value.participantId());
    node.put("role", value.role().name());
    node.put("incarnation", value.incarnation().toString());
    node.put("registrationEpoch", value.registrationEpoch());
    return node;
  }

  private static Participant readParticipant(JsonNode node) throws IOException {
    fields(node, "participantId", "role", "incarnation", "registrationEpoch");
    return new Participant(string(node, "participantId"), Role.valueOf(string(node, "role")),
        uuid(node, "incarnation"), number(node, "registrationEpoch"));
  }

  private static void fields(JsonNode node, String... names) throws IOException {
    require(node != null && node.isObject(), "Expected manifest object");
    Set<String> actual = new HashSet<>();
    node.fieldNames().forEachRemaining(actual::add);
    require(actual.equals(Set.of(names)), "Unexpected or missing manifest fields");
  }

  private static String string(JsonNode node, String field) throws IOException {
    JsonNode value = node.get(field);
    require(value != null && value.isTextual(), "Expected manifest string");
    return value.textValue();
  }

  private static UUID uuid(JsonNode node, String field) throws IOException {
    String value = string(node, field);
    UUID uuid = UUID.fromString(value);
    require(uuid.toString().equals(value), "Expected canonical UUID");
    return uuid;
  }

  private static long number(JsonNode node, String field) throws IOException {
    JsonNode value = node.get(field);
    require(value != null && value.isIntegralNumber() && value.canConvertToLong(), "Expected manifest integer");
    return value.longValue();
  }

  private static void require(boolean condition, String message) throws IOException {
    if (!condition) {
      throw new IOException(message);
    }
  }
}
