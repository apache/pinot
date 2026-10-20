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
package org.apache.pinot.minion.coordination;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.coordination.NativeTaskOutputIdentity;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.JsonUtils;

import static com.google.common.base.Preconditions.checkArgument;


/// Generates one genuine OFFLINE segment from a digest-pinned JSON-lines file. Local root approvals constrain
/// input reads and immutable staging. No legacy upload, replacement, deletion, or metadata publication is invoked.
public final class NativeSegmentGenerationTask {
  private final Path _sourceRoot;
  private final Path _outputRoot;
  private final long _maxSourceBytes;
  private final int _maxRows;

  public NativeSegmentGenerationTask(Path sourceRoot, Path outputRoot, long maxSourceBytes, int maxRows)
      throws IOException {
    _sourceRoot = sourceRoot.toRealPath();
    Files.createDirectories(outputRoot);
    _outputRoot = outputRoot.toRealPath();
    checkArgument(Files.isDirectory(_sourceRoot) && Files.isDirectory(_outputRoot), "Task roots must be directories");
    checkArgument(maxSourceBytes > 0 && maxSourceBytes <= 64 * 1024 * 1024 && maxRows > 0 && maxRows <= 1_000_000,
        "Native generation requires bounded source bytes and rows");
    _maxSourceBytes = maxSourceBytes;
    _maxRows = maxRows;
  }

  public MetadataRpc.OfflineSegmentContent generate(MetadataRpc.ParticipantToken participant,
      MetadataRpc.TaskExecutionState task, TableConfig table, Schema schema, Path workingDirectory, Runnable guard)
      throws Exception {
    checkArgument(NativeTaskOutputIdentity.TASK_TYPE.equals(task.getInput().getQueue().getTaskType())
        && task.getInput().hasOutputPolicy() && task.getInput().getOutputPolicy().getMaxOutputs() == 1,
        "Only one-output native segment generation is supported");
    checkArgument(table.getTableType() == TableType.OFFLINE
        && table.getTableName().equals(task.getInput().getQueue().getTableName()), "Task requires its OFFLINE table");
    checkArgument(Path.of(NativeTaskOutputIdentity.outputRoot(task.getInput().getOutputPolicy().getRootUri()))
        .toRealPath().equals(_outputRoot), "Task output root is not locally approved");
    Map<String, String> configs = task.getInput().getConfigsMap();
    checkArgument(configs.keySet().equals(Set.of("sourceUri", "sourceSha256")), "Unsupported generation options");
    String expectedDigest = configs.get("sourceSha256");
    checkArgument(expectedDigest.matches("[a-f0-9]{64}"), "Source SHA-256 is required");
    URI sourceUri = URI.create(configs.get("sourceUri"));
    checkArgument("file".equals(sourceUri.getScheme()) && sourceUri.getAuthority() == null
        && sourceUri.getQuery() == null && sourceUri.getFragment() == null,
        "Only approved local sources are supported");
    Path source = Path.of(sourceUri).toRealPath();
    checkArgument(source.startsWith(_sourceRoot) && Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS),
        "Task source is outside the approved root");
    Files.createDirectories(workingDirectory);
    Path temporary = Files.createTempDirectory(workingDirectory, "generation-");
    try {
      guard.run();
      Path captured = temporary.resolve("input.json");
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      try (InputStream input = Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS);
          var output = Files.newOutputStream(captured, StandardOpenOption.CREATE_NEW)) {
        byte[] buffer = new byte[8192];
        long size = 0;
        for (int count; (count = input.read(buffer)) != -1;) {
          guard.run();
          size += count;
          checkArgument(size <= _maxSourceBytes, "Native task source exceeds its byte limit");
          digest.update(buffer, 0, count);
          output.write(buffer, 0, count);
        }
      }
      checkArgument(HexFormat.of().formatHex(digest.digest()).equals(expectedDigest), "Source digest differs");
      UUID taskId = UUID.fromString(task.getTaskId());
      UUID history = UUID.fromString(participant.getHistoryId());
      String segmentName = NativeTaskOutputIdentity.outputSegmentName(taskId, task.getAttempt(), 0);
      Path generated = Files.createDirectory(temporary.resolve("generated"));
      SegmentGeneratorConfig generator = new SegmentGeneratorConfig(table, schema);
      generator.setOutDir(generated.toString());
      generator.setSegmentName(segmentName);
      generator.setContinueOnError(false);
      generator.setFailOnEmptySegment(true);
      try (JsonLinesReader reader = new JsonLinesReader(captured, _maxRows, guard)) {
        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(generator, reader);
        driver.build();
      }
      guard.run();
      Path segment = generated.resolve(segmentName);
      SegmentMetadataImpl metadata = new SegmentMetadataImpl(segment.toFile());
      Path archive = temporary.resolve("output.tar.gz");
      TarCompressionUtils.createCompressedTarFile(segment.toFile(), archive.toFile());
      digest.reset();
      try (InputStream input = Files.newInputStream(archive)) {
        byte[] buffer = new byte[8192];
        for (int count; (count = input.read(buffer)) != -1;) {
          guard.run();
          digest.update(buffer, 0, count);
        }
      }
      String sha256 = HexFormat.of().formatHex(digest.digest());
      String uri = NativeTaskOutputIdentity.outputUri(task.getInput().getOutputPolicy().getRootUri(), history,
          taskId, task.getAttempt(), UUID.fromString(participant.getIncarnation()), 0, sha256);
      Path policyRoot = Path.of(NativeTaskOutputIdentity.outputRoot(task.getInput().getOutputPolicy().getRootUri()));
      Path destination = _outputRoot.resolve(policyRoot.relativize(Path.of(URI.create(uri))));
      createOutputDirectories(destination.getParent());
      // CREATE_NEW prevents a retry or stale process from overwriting any attempt's artifact.
      try (FileChannel output = FileChannel.open(destination, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
          InputStream input = Files.newInputStream(archive)) {
        byte[] buffer = new byte[8192];
        for (int count; (count = input.read(buffer)) != -1;) {
          guard.run();
          ByteBuffer bytes = ByteBuffer.wrap(buffer, 0, count);
          while (bytes.hasRemaining()) {
            output.write(bytes);
          }
        }
        output.force(true);
      }
      // Persist directory entries through the approved root before metadata can make the archive reachable.
      for (Path directory = destination.getParent(); directory.startsWith(_outputRoot);
          directory = directory.getParent()) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
          channel.force(true);
        }
      }
      guard.run();
      return MetadataRpc.OfflineSegmentContent.newBuilder().setTableName(table.getTableName())
          .setTableObjectId(task.getInput().getQueue().getTableObjectId()).setSegmentName(segmentName)
          .setSegmentObjectId(NativeTaskOutputIdentity.outputObjectId(history, taskId, task.getAttempt(), 0).toString())
          .setContentSha256(sha256).setDownloadUri(uri).setLoader(MetadataRpc.SegmentLoaderDescriptor.newBuilder()
              .setCrc(metadata.getCrc()).setCreationTimeMs(metadata.getIndexCreationTime()).setPushTimeMs(-1)).build();
    } finally {
      FileUtils.deleteDirectory(temporary.toFile());
    }
  }

  private void createOutputDirectories(Path parent) throws IOException {
    checkArgument(parent.normalize().startsWith(_outputRoot), "Output escaped approved root");
    Path current = _outputRoot;
    for (Path component : _outputRoot.relativize(parent)) {
      current = current.resolve(component);
      if (!Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
        Files.createDirectory(current);
      }
      checkArgument(Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)
          && current.toRealPath().startsWith(_outputRoot), "Output directory contains a symbolic link");
    }
  }

  /// The driver may rewind for dictionary/index construction. Each pass validates the same captured input bounds.
  private static final class JsonLinesReader implements RecordReader {
    private final Path _file;
    private final int _maxRows;
    private final Runnable _guard;
    private BufferedReader _reader;
    @Nullable
    private String _next;
    private int _rows;

    private JsonLinesReader(Path file, int maxRows, Runnable guard) throws IOException {
      _file = file;
      _maxRows = maxRows;
      _guard = guard;
      rewind();
    }

    @Override
    public void init(File file, Set<String> fields, RecordReaderConfig config) {
      throw new UnsupportedOperationException("Native reader is initialized with its captured file");
    }

    @Override
    public boolean hasNext() {
      _guard.run();
      if (_next == null) {
        try {
          _next = _reader.readLine();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      return _next != null;
    }

    @Override
    public GenericRow next(GenericRow reuse) throws IOException {
      _guard.run();
      checkArgument(_next != null && ++_rows <= _maxRows, "Native task exceeds its row limit");
      Map<String, Object> row = JsonUtils.stringToMap(_next);
      _next = null;
      reuse.clear();
      reuse.putValues(row);
      return reuse;
    }

    @Override
    public void rewind() throws IOException {
      close();
      _reader = Files.newBufferedReader(_file, StandardCharsets.UTF_8);
      _next = null;
      _rows = 0;
    }

    @Override
    public void close() throws IOException {
      if (_reader != null) {
        _reader.close();
      }
    }
  }
}
