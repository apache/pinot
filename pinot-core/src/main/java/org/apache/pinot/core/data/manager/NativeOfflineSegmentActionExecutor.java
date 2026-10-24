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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Authority;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Installation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Mode;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ReadGuard;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Request;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Result;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Status;
import org.apache.pinot.common.metadata.segment.SegmentLoadMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.data.manager.offline.NativeImmutableSegmentDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


/// Native-only physical load/reload using Pinot's normal segment preprocessing and opening. Immutable candidate
/// directories preserve the paths held by old query readers. This class performs no consensus or serving publication.
final class NativeOfflineSegmentActionExecutor implements NativeOfflineSegmentAction.Executor {
  private final BaseTableDataManager _manager;
  private final Path _root;
  private boolean _rootDurable;

  NativeOfflineSegmentActionExecutor(BaseTableDataManager manager) {
    _manager = manager;
    _root = manager._indexDir.toPath().resolve(".native");
  }

  @Nullable
  @Override
  public Installation installed(String segmentName) throws IOException {
    NativeOfflineSegmentManifest manifest = readActive(segmentName);
    return manifest == null ? null : manifest.installation();
  }

  @Override
  public Result execute(Request request, Authority authority) throws Exception {
    requireNonNull(authority, "authority");
    validate(request);
    ensureRootDurable();
    String segmentName = request.identity().segmentName();
    NativeOfflineSegmentManifest before = readActive(segmentName);
    boolean replay = before != null && before.matches(request);
    // Missing reload targets never turn into implicit loads. The final check below still decides absence/fencing.
    boolean missingReload = request.mode() == Mode.RELOAD && before == null;
    Path candidate = null;
    ImmutableSegment prepared = null;
    NativeOfflineSegmentManifest manifest = null;
    boolean selected = replay;
    boolean registered = false;
    AtomicBoolean ownershipTransferred = new AtomicBoolean();
    try {
      IndexLoadingConfig config = loadingConfig(request);
      SegmentZKMetadata metadata = BaseTableDataManager.legacyLoadingValue(request.snapshot().segment());
      if (replay) {
        manifest = before;
        candidate = manifest.installationDirectory(_root);
        Path segment = candidate.resolve("segment");
        NativeOfflineSegmentManifest copy = NativeOfflineSegmentManifest.read(candidate.resolve("manifest.json"));
        require(copy.manifestSha256().equals(manifest.manifestSha256()), "Selected installation manifest differs");
        manifest.verifyFiles(segment);
        prepared = ImmutableSegmentLoader.load(segment.toFile(), config, false,
            _manager._segmentOperationsThrottlerSet, metadata);
      } else if (!missingReload) {
        UUID installationId = UUID.randomUUID();
        candidate = _root.resolve("materializations").resolve(request.identity().historyId().toString())
            .resolve(request.identity().table().objectId().toString())
            .resolve(request.identity().content().objectId().toString()).resolve(installationId.toString());
        NativeOfflineSegmentManifest.createDirectories(candidate);
        Path segment;
        if (request.mode() == Mode.RECONCILE && !request.forceDownload() && before != null
            && reusable(before.identity(), request.identity())) {
          Path previous = before.installationDirectory(_root);
          NativeOfflineSegmentManifest copy = NativeOfflineSegmentManifest.read(previous.resolve("manifest.json"));
          require(copy.manifestSha256().equals(before.manifestSha256()), "Selected installation manifest differs");
          segment = candidate.resolve("segment");
          // Copy verified prepared files into a new permanent directory. Old readers retain their original paths,
          // while the new process receives a fresh manifest and receipt bound to its current assignment.
          before.copyVerifiedFiles(previous.resolve("segment"), segment);
        } else {
          segment = prepareBytes(request, candidate);
          ImmutableSegmentLoader.preprocess(segment.toFile(), config, _manager._segmentOperationsThrottlerSet,
              metadata);
        }
        manifest = NativeOfflineSegmentManifest.prepare(request, installationId, segment);
        manifest.write(candidate.resolve("manifest.json"));
        // Open only after the directory has reached its permanent path. No directory is moved after this point.
        prepared = ImmutableSegmentLoader.load(segment.toFile(), config, false,
            _manager._segmentOperationsThrottlerSet, metadata);
      }

      // Bind the prepared bytes, exact installation identity and normalized captured schema before publication.
      // Schema copies are made outside the final segment lock and never read from the independently refreshed cache.
      NativeImmutableSegmentDataManager preparedManager = prepared == null ? null
          : new NativeImmutableSegmentDataManager(prepared, requireNonNull(manifest).identity(), config.getSchema());
      authority.refresh();
      Lock lock = _manager.getSegmentLock(segmentName);
      lock.lock();
      try (ReadGuard guard = requireNonNull(authority.acquire(request.identity()), "readGuard")) {
        if (!guard.isCurrent() || _manager.isShutDown()) {
          return new Result(Status.FENCED, null);
        }
        NativeOfflineSegmentManifest current = readActive(segmentName);
        UUID currentId = current == null ? null : current.installationId();
        if (replay) {
          if (!Objects.equals(currentId, before.installationId()) || !current.matches(request)) {
            return new Result(Status.FENCED, null);
          }
        } else {
          if (!Objects.equals(request.expectedInstallationId(), currentId)) {
            return new Result(Status.FENCED, null);
          }
          if (current == null && _manager.hasSegment(segmentName)) {
            // Unmanifested bytes from another load path have no native installation provenance.
            return new Result(Status.FENCED, null);
          }
          if (request.mode() == Mode.LOAD && current != null) {
            return new Result(Status.FENCED, null);
          }
          if (request.mode() == Mode.RELOAD && current == null) {
            return new Result(Status.ABSENT, null);
          }
          if (missingReload || (current != null
              && (!current.identity().historyId().equals(request.identity().historyId())
              || !current.identity().table().objectId().equals(request.identity().table().objectId())
              || !current.identity().server().participantId().equals(request.identity().server().participantId())
              || current.identity().server().registrationEpoch() > request.identity().server().registrationEpoch()
              || current.identity().server().registrationEpoch() == request.identity().server().registrationEpoch()
              && !current.identity().server().equals(request.identity().server())))) {
            return new Result(Status.FENCED, null);
          }
        }
        require(preparedManager != null && manifest != null, "Prepared native installation is missing");
        // Retain candidate files even when publication throws after rename; the selected manifest may be durable.
        selected = true;
        registered = _manager.publishNativeSegment(preparedManager, manifest, activePath(segmentName),
            ownershipTransferred);
        if (!registered) {
          selected = replay;
        }
        return registered ? new Result(Status.INSTALLED, manifest.installation()) : new Result(Status.FENCED, null);
      } finally {
        lock.unlock();
      }
    } finally {
      if (prepared != null && !ownershipTransferred.get()) {
        prepared.destroy();
      }
      if (candidate != null && !selected) {
        FileUtils.deleteQuietly(candidate.toFile());
      }
    }
  }

  private synchronized void ensureRootDurable() throws IOException {
    if (!_rootDurable) {
      NativeOfflineSegmentManifest.createDirectories(_root);
      NativeOfflineSegmentManifest.forceAncestors(_root);
      _rootDurable = true;
    }
  }

  private static boolean reusable(Identity installed, Identity requested) {
    return installed.historyId().equals(requested.historyId()) && installed.tableName().equals(requested.tableName())
        && installed.table().equals(requested.table()) && installed.schemaName().equals(requested.schemaName())
        && installed.schema().equals(requested.schema()) && installed.segmentName().equals(requested.segmentName())
        && installed.content().equals(requested.content())
        && installed.contentSha256().equals(requested.contentSha256());
  }

  private void validate(Request request) {
    requireNonNull(request, "request");
    checkArgument(request.identity().tableName().equals(_manager.getTableName()), "Native action table differs");
    TableConfig table = request.snapshot().tableConfig();
    BaseTableDataManager.validateNativeTable(_manager._instanceDataManagerConfig, table);
    String loader = _manager._instanceDataManagerConfig.getSegmentDirectoryLoader();
    checkArgument(SegmentDirectoryLoaderRegistry.DEFAULT_SEGMENT_DIRECTORY_LOADER_NAME.equals(loader),
        "Native physical installation requires the default local segment loader");
    checkArgument((table.getTierConfigsList() == null || table.getTierConfigsList().isEmpty())
        && request.snapshot().segment().tier() == null
        && (_manager._instanceDataManagerConfig.getTierConfigs() == null
        || _manager._instanceDataManagerConfig.getTierConfigs().isEmpty()),
        "Native tier installation is not supported");
    SegmentLoadMetadata segment = request.snapshot().segment();
    checkArgument(segment.published() && segment.crc() >= 0 && (!segment.useDataCrc() || segment.dataCrc() >= 0),
        "Native content must be published with valid CRC metadata");
    checkArgument(!CommonConstants.Segment.METADATA_URI_FOR_PEER_DOWNLOAD.equals(segment.downloadUri()),
        "Native peer download is not supported");
  }

  private IndexLoadingConfig loadingConfig(Request request) {
    TableConfig table = BaseTableDataManager.copyNativeTableConfig(request.snapshot().tableConfig());
    Schema source = request.snapshot().schema();
    Schema schema = Schema.cloneSchemaWithName(source, source.getSchemaName());
    IndexLoadingConfig config = new IndexLoadingConfig(_manager._instanceDataManagerConfig, table, schema);
    config.setTableDataDir(_manager._tableDataDir);
    return config;
  }

  private Path prepareBytes(Request request, Path candidate) throws Exception {
    boolean tablePermit = false;
    boolean instancePermit = false;
    try {
      if (_manager._segmentDownloadSemaphore != null) {
        _manager._segmentDownloadSemaphore.acquire();
        tablePermit = true;
      }
      if (_manager._segmentOperationsThrottlerSet != null) {
        _manager._segmentOperationsThrottlerSet.getSegmentDownloadThrottler().acquire();
        instancePermit = true;
      }
      SegmentLoadMetadata descriptor = request.snapshot().segment();
      Path blob = candidate.resolve("download.blob");
      SegmentFetcherFactory.fetchSegmentToLocal(descriptor.downloadUri(), blob.toFile());
      require(NativeOfflineSegmentManifest.digest(blob).equals(request.identity().contentSha256()),
          "Downloaded native segment SHA-256 differs");
      Path archive = blob;
      if (descriptor.crypterName() != null) {
        archive = candidate.resolve("decrypted.tar.gz");
        PinotCrypterFactory.create(descriptor.crypterName()).decrypt(blob.toFile(), archive.toFile());
      }
      Path extracted = candidate.resolve("extracted");
      NativeOfflineSegmentManifest.createDirectories(extracted);
      TarCompressionUtils.untar(archive.toFile(), extracted.toFile());
      List<Path> roots;
      try (Stream<Path> paths = Files.list(extracted)) {
        roots = paths.collect(Collectors.toList());
      }
      require(roots.size() == 1 && Files.isDirectory(roots.get(0), LinkOption.NOFOLLOW_LINKS),
          "Native segment archive must contain exactly one segment directory");
      Path segment = candidate.resolve("segment");
      Files.move(roots.get(0), segment, StandardCopyOption.ATOMIC_MOVE);
      SegmentMetadataImpl content = new SegmentMetadataImpl(segment.toFile());
      require(content.getName().equals(request.identity().segmentName()) && content.getCrc() == descriptor.crc()
          && (!descriptor.useDataCrc() || content.getDataCrc() == descriptor.dataCrc()),
          "Downloaded segment name or CRC metadata differs");
      Files.delete(extracted);
      Files.delete(blob);
      if (!archive.equals(blob)) {
        Files.delete(archive);
      }
      return segment;
    } finally {
      if (instancePermit) {
        _manager._segmentOperationsThrottlerSet.getSegmentDownloadThrottler().release();
      }
      if (tablePermit) {
        _manager._segmentDownloadSemaphore.release();
      }
    }
  }

  @Nullable
  private NativeOfflineSegmentManifest readActive(String segmentName) throws IOException {
    Path path = activePath(segmentName);
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      return null;
    }
    NativeOfflineSegmentManifest manifest = NativeOfflineSegmentManifest.read(path);
    require(manifest.identity().segmentName().equals(segmentName)
        && manifest.identity().tableName().equals(_manager.getTableName()), "Selected native segment identity differs");
    return manifest;
  }

  private Path activePath(String segmentName) {
    String name = NativeOfflineSegmentManifest.digest(segmentName.getBytes(StandardCharsets.UTF_8)) + ".json";
    return _root.resolve("active").resolve(name);
  }

  private static void require(boolean condition, String message) throws IOException {
    if (!condition) {
      throw new IOException(message);
    }
  }
}
