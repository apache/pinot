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
package org.apache.pinot.core.util.trace;

import com.google.common.annotations.VisibleForTesting;
import java.lang.management.ManagementFactory;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContinuousJfrStarter implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousJfrStarter.class);
  private static final String JFR_CONFIGURE_COMMAND = "jfrConfigure";
  private static final String JFR_START_COMMAND = "jfrStart";
  private static final String JFR_STOP_COMMAND = "jfrStop";
  private static final String JFR_DIAGNOSTIC_MBEAN = "com.sun.management:type=DiagnosticCommand";
  private static final Duration REPOSITORY_CLEANUP_INTERVAL = Duration.ofDays(1);
  private static final ScheduledExecutorService REPOSITORY_CLEANUP_EXECUTOR =
      Executors.newSingleThreadScheduledExecutor(
      runnable -> {
        Thread thread = new Thread(runnable, "pinot-jfr-repository-cleaner");
        thread.setDaemon(true);
        return thread;
      });

  /// Key that controls whether to enable continuous JFR recording.
  public static final String ENABLED = "enabled";
  /// Default value for the enabled key.
  public static final boolean DEFAULT_ENABLED = false;

  /// Key that controls the JFR configuration to use. Valid values are 'default' or 'profile', but more can be added
  /// by adding custom configurations/settings. See the JFR documentation for more information.
  ///
  /// The default value is 'default', which is the default JFR configuration and the one that has a target of less
  /// than 1% overhead.
  public static final String CONFIGURATION = "configuration";
  /// Default value for the configuration key.
  public static final String DEFAULT_CONFIGURATION = "default";
  /// Key that controls the name of the recording.
  ///
  /// This is used to identify the recording in the JFR UI.
  /// The default value is 'pinot-continuous'.
  public static final String NAME = "name";
  /// Default value for the name key.
  public static final String DEFAULT_NAME = "pinot-continuous";

  /// Key that controls whether to dump the recording on exit.
  public static final String DUMP_ON_EXIT = "dumpOnExit";
  /// Default value for the dumpOnExit key.
  ///
  /// If true, the recording will be dumped to a file when the JVM exits.
  /// We default to false so JFR does not force an on-exit dump file.
  /// A side effect of setting this to true is that the repository may be deleted even if
  /// [#PRESERVE_REPOSITORY] is true.
  public static final boolean DEFAULT_DUMP_ON_EXIT = false;
  /// Key that controls the JFR repository directory path.
  ///
  /// The default value is the tmp directory of the system, as determined by the `java.io.tmpdir` system property.
  ///
  /// If set, this is applied via the JFR DiagnosticCommand MBean as `repositorypath`.
  public static final String DIRECTORY = "directory";
  /// Key that controls the JFR default dump directory path.
  /// If set, this is applied via the JFR DiagnosticCommand MBean as `dumppath`.
  public static final String DUMP_PATH = "dumpPath";
  /// Key that controls whether the JFR repository directory is preserved after the JVM exits.
  ///
  /// By default, JFR deletes the repository directory on JVM exit. Setting this to true prevents that, which is
  /// useful when the repository is shared or when post-mortem analysis of in-flight chunks is needed.
  ///
  /// If set, this is applied via the JFR DiagnosticCommand MBean as `preserve-repository`.
  public static final String PRESERVE_REPOSITORY = "preserveRepository";
  /// Default value for the preserveRepository key.
  public static final boolean DEFAULT_PRESERVE_REPOSITORY = true;
  /// Key that controls the maximum total size for all repositories under the configured repository directory.
  ///
  /// When the total size exceeds this threshold, the older repositories are removed first. The repository currently
  /// used by JFR is always kept, even if it is larger than this threshold.
  public static final String REPOSITORY_MAX_TOTAL_SIZE = "repositoryMaxTotalSize";
  /// Default maximum total size for all repositories under the configured repository directory.
  public static final String DEFAULT_REPOSITORY_MAX_TOTAL_SIZE = "20GB";

  /// Key that controls whether to buffer the recording to disk.
  /// If false, the recording will only be kept in memory.
  /// If true, the recording repository will persist data on disk.
  public static final String TO_DISK = "toDisk";
  /// Default value for the toDisk key.
  public static final boolean DEFAULT_TO_DISK = true;
  /// Key that controls the maximum size of the recording file.
  /// Once the file reaches this size, older events will be discarded.
  /// If both maxSize and maxAge are set, the recording will be discarded when either condition is met.
  ///
  /// This is only used if toDisk is true.
  /// The value is in bytes.
  /// The default value is 2G. Valid values are human readable data size strings, as defined by the DataSizeUtils class
  /// (e.g. '10.5G', '40B') or data size in bytes (e.g. '2147483648').
  ///
  /// @see #MAX_AGE
  public static final String MAX_SIZE = "maxSize";
  public static final String DEFAULT_MAX_SIZE = "2GB";
  /// Key that controls the maximum age of the recording file.
  /// Once the file reaches this age, older events will be discarded.
  /// If both maxSize and maxAge are set, the recording will be discarded when either condition is met.
  ///
  /// This is only used if toDisk is true.
  /// The value is a duration string, as defined by the Duration class.
  /// The default value is 7 days (P7D).
  ///
  /// @see #MAX_SIZE
  public static final String MAX_AGE = "maxAge";
  public static final String DEFAULT_MAX_AGE = "P7D";

  /// A flag to track whether the JFR recording has been started.
  /// This is specially useful for testing and quickstarts, where servers, brokers and other components are executed
  /// in the same JVM.
  public static final ContinuousJfrStarter INSTANCE = new ContinuousJfrStarter();
  private static final Object GLOBAL_RECORDING_LOCK = new Object();
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private static final Map<String, RecordingState> GLOBAL_RECORDINGS = new HashMap<>();
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean _running = false;
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private Map<String, Object> _currentConfig;
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private String _recordingName;
  private final MBeanServer _mBeanServer;
  @Nullable
  private final ObjectName _diagnosticCommandObjectName;

  @VisibleForTesting
  protected ContinuousJfrStarter() {
    this(ManagementFactory.getPlatformMBeanServer(), createDiagnosticCommandObjectName());
  }

  @VisibleForTesting
  protected ContinuousJfrStarter(MBeanServer mBeanServer, @Nullable ObjectName diagnosticCommandObjectName) {
    _mBeanServer = mBeanServer;
    _diagnosticCommandObjectName = diagnosticCommandObjectName;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    boolean jfrChanged = changedConfigs.stream()
        .anyMatch(changedConfig -> changedConfig.startsWith(CommonConstants.JFR));
    if (!jfrChanged && _currentConfig != null) {
      LOGGER.debug("ChangedConfigs: {} does not contain any JFR config. Skipping updates", changedConfigs);
      return;
    }
    PinotConfiguration config = new PinotConfiguration(clusterConfigs);
    PinotConfiguration subset = config.subset(CommonConstants.JFR);

    synchronized (GLOBAL_RECORDING_LOCK) {
      Map<String, Object> newSubsetMap = subset.toMap();

      if (_currentConfig != null && _currentConfig.equals(newSubsetMap)) {
        // No change
        LOGGER.debug("JFR config change detected, but no actual change in config");
        return;
      }

      if (!applyConfig(subset, newSubsetMap)) {
        LOGGER.warn("Failed to apply continuous JFR config update");
      }
    }
  }

  public boolean isRunning() {
    synchronized (GLOBAL_RECORDING_LOCK) {
      return _running;
    }
  }

  @VisibleForTesting
  protected static void resetGlobalStateForTesting() {
    synchronized (GLOBAL_RECORDING_LOCK) {
      GLOBAL_RECORDINGS.clear();
    }
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean applyConfig(PinotConfiguration subset, Map<String, Object> newSubsetMap) {
    boolean enabled = subset.getProperty(ENABLED, DEFAULT_ENABLED);
    if (!enabled) {
      if (!releaseRecordingReference()) {
        return false;
      }
      _currentConfig = newSubsetMap;
      return true;
    }
    String targetRecordingName = subset.getProperty(NAME, DEFAULT_NAME);
    boolean alreadyAttachedToTarget = _running && targetRecordingName.equals(_recordingName);
    if (!alreadyAttachedToTarget && !releaseRecordingReference()) {
      return false;
    }
    if (!acquireRecordingReference(subset, newSubsetMap, targetRecordingName, alreadyAttachedToTarget)) {
      return false;
    }
    _currentConfig = newSubsetMap;
    return true;
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean acquireRecordingReference(PinotConfiguration subset, Map<String, Object> configMap,
      String recordingName, boolean alreadyAttachedToTarget) {
    RecordingState recordingState = GLOBAL_RECORDINGS.get(recordingName);
    if (recordingState == null) {
      if (!startRecording(subset, recordingName)) {
        return false;
      }
      recordingState = new RecordingState(new HashMap<>(configMap), 1);
      updateRepositoryCleanupTask(subset, recordingName, recordingState);
      GLOBAL_RECORDINGS.put(recordingName, recordingState);
      _recordingName = recordingName;
      _running = true;
      return true;
    }
    if (!recordingState._config.equals(configMap)) {
      if (!restartRecording(subset, recordingName)) {
        LOGGER.warn("Failed to restart continuous JFR recording '{}' for config update", recordingName);
        return false;
      }
      recordingState._config = new HashMap<>(configMap);
      updateRepositoryCleanupTask(subset, recordingName, recordingState);
    }
    if (!alreadyAttachedToTarget) {
      recordingState._referenceCount++;
    }
    _recordingName = recordingName;
    _running = true;
    return true;
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean restartRecording(PinotConfiguration subset, String recordingName) {
    if (!stopRecording(recordingName)) {
      LOGGER.warn("Failed to stop continuous JFR recording '{}' before restart", recordingName);
      return false;
    }
    return startRecording(subset, recordingName);
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean releaseRecordingReference() {
    if (!_running) {
      return true;
    }
    assert _recordingName != null;
    RecordingState recordingState = GLOBAL_RECORDINGS.get(_recordingName);
    if (recordingState == null) {
      LOGGER.warn("No global state found for running continuous JFR recording '{}'", _recordingName);
      _running = false;
      _recordingName = null;
      return true;
    }
    if (recordingState._referenceCount > 1) {
      recordingState._referenceCount--;
      _running = false;
      _recordingName = null;
      return true;
    }
    if (!stopRecording(_recordingName)) {
      return false;
    }
    cancelRepositoryCleanupTask(recordingState);
    GLOBAL_RECORDINGS.remove(_recordingName);
    _running = false;
    _recordingName = null;
    return true;
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean stopRecording(String recordingName) {
    if (!isDiagnosticCommandAvailable()) {
      LOGGER.warn("JFR DiagnosticCommand MBean is unavailable. Cannot stop continuous JFR recording '{}'",
          recordingName);
      return false;
    }
    if (!executeDiagnosticCommand(JFR_STOP_COMMAND, "name=" + recordingName)) {
      LOGGER.warn("Failed to stop continuous JFR recording '{}'", recordingName);
      return false;
    }
    LOGGER.info("Stopped continuous JFR recording {}", recordingName);
    return true;
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean startRecording(PinotConfiguration subset, String recordingName) {
    if (!subset.getProperty(ENABLED, DEFAULT_ENABLED)) {
      LOGGER.info("Continuous JFR recording is disabled");
      return true;
    }
    if (!isDiagnosticCommandAvailable()) {
      LOGGER.warn("JFR DiagnosticCommand MBean is unavailable. Cannot start continuous JFR recording");
      return false;
    }
    if (!applyRuntimeOptions(subset)) {
      LOGGER.warn("Failed to apply JFR runtime options for recording '{}'", recordingName);
      return false;
    }

    String maxAge = subset.getProperty(MAX_AGE, DEFAULT_MAX_AGE);
    try {
      List<String> startArguments = new ArrayList<>();
      startArguments.add("name=" + recordingName);
      startArguments.add("settings=" + subset.getProperty(CONFIGURATION, DEFAULT_CONFIGURATION));
      startArguments.add("dumponexit=" + subset.getProperty(DUMP_ON_EXIT, DEFAULT_DUMP_ON_EXIT));
      boolean toDisk = subset.getProperty(TO_DISK, DEFAULT_TO_DISK);
      startArguments.add("disk=" + toDisk);
      if (toDisk) {
        try {
          long maxSize = DataSizeUtils.toBytes(subset.getProperty(MAX_SIZE, DEFAULT_MAX_SIZE));
          startArguments.add("maxsize=" + maxSize);
          startArguments.add("maxage=" + toJfrTimeArgument(maxAge));
        } catch (IllegalArgumentException | ClassCastException e) {
          throw new RuntimeException("Failed to parse maxSize configuration for continuous JFR recording '"
              + recordingName + "'", e);
        }
      }
      if (!executeDiagnosticCommand(JFR_START_COMMAND, startArguments.toArray(new String[0]))) {
        LOGGER.warn("Failed to start continuous JFR recording '{}'", recordingName);
        return false;
      }
    } catch (DateTimeParseException e) {
      throw new RuntimeException("Failed to parse duration '" + maxAge + "'", e);
    }

    LOGGER.info("Started continuous JFR recording {} with configuration: {}", recordingName, subset);
    return true;
  }

  @VisibleForTesting
  protected boolean executeDiagnosticCommand(String operationName, String... arguments) {
    try {
      _mBeanServer.invoke(_diagnosticCommandObjectName, operationName, new Object[]{arguments},
          new String[]{String[].class.getName()});
      return true;
    } catch (Exception e) {
      LOGGER.warn("Failed to execute JFR command '{}' with arguments {}", operationName, Arrays.toString(arguments), e);
      return false;
    }
  }

  private boolean applyRuntimeOptions(PinotConfiguration subset) {
    List<String> configureArguments = new ArrayList<>();
    String repositoryPath = subset.getProperty(DIRECTORY, (String) null);
    if (repositoryPath != null && !repositoryPath.isEmpty()) {
      configureArguments.add("repositorypath=" + repositoryPath);
    }
    String dumpPath = subset.getProperty(DUMP_PATH, (String) null);
    if (dumpPath != null && !dumpPath.isEmpty()) {
      configureArguments.add("dumppath=" + dumpPath);
    }
    if (configureArguments.isEmpty()) {
      return true;
    }
    boolean preserveRepository = subset.getProperty(PRESERVE_REPOSITORY, DEFAULT_PRESERVE_REPOSITORY);
    if (preserveRepository) {
      List<String> configureArgumentsWithPreserve = new ArrayList<>(configureArguments);
      configureArgumentsWithPreserve.add("preserve-repository=true");
      if (executeDiagnosticCommand(JFR_CONFIGURE_COMMAND, configureArgumentsWithPreserve.toArray(new String[0]))) {
        return true;
      }
      LOGGER.warn("Failed to apply preserveRepository option for continuous JFR recording. Retrying without it");
    }
    return executeDiagnosticCommand(JFR_CONFIGURE_COMMAND, configureArguments.toArray(new String[0]));
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private void updateRepositoryCleanupTask(
      PinotConfiguration subset, String recordingName, RecordingState recordingState) {
    if (!isRepositoryCleanupEnabled()) {
      return;
    }
    String repositoryRoot = subset.getProperty(DIRECTORY, (String) null);
    if (repositoryRoot == null || repositoryRoot.isEmpty()) {
      cancelRepositoryCleanupTask(recordingState);
      return;
    }
    long maxTotalSizeBytes;
    try {
      maxTotalSizeBytes = DataSizeUtils.toBytes(subset.getProperty(REPOSITORY_MAX_TOTAL_SIZE,
          DEFAULT_REPOSITORY_MAX_TOTAL_SIZE));
    } catch (IllegalArgumentException | ClassCastException e) {
      throw new RuntimeException("Failed to parse continuous JFR repository max total size from key '"
          + REPOSITORY_MAX_TOTAL_SIZE + "'", e);
    }
    if (recordingState._cleanupTask != null && !recordingState._cleanupTask.isCancelled()
        && Objects.equals(recordingState._repositoryRoot, repositoryRoot)
        && recordingState._repositoryMaxTotalSizeBytes == maxTotalSizeBytes) {
      return;
    }
    cancelRepositoryCleanupTask(recordingState);
    recordingState._repositoryRoot = repositoryRoot;
    recordingState._repositoryMaxTotalSizeBytes = maxTotalSizeBytes;
    recordingState._cleanupTask = REPOSITORY_CLEANUP_EXECUTOR.scheduleWithFixedDelay(
        () -> cleanupRepositoriesIfNeeded(recordingName, repositoryRoot, maxTotalSizeBytes), 0,
        REPOSITORY_CLEANUP_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private void cancelRepositoryCleanupTask(RecordingState recordingState) {
    if (recordingState._cleanupTask != null) {
      recordingState._cleanupTask.cancel(false);
      recordingState._cleanupTask = null;
    }
  }

  @VisibleForTesting
  protected boolean isRepositoryCleanupEnabled() {
    return true;
  }

  private void cleanupRepositoriesIfNeeded(String recordingName, String repositoryRoot, long maxTotalSizeBytes) {
    try {
      Path repositoryRootPath = Path.of(repositoryRoot).toAbsolutePath().normalize();
      Path activeRepositoryPath = resolveCurrentRepositoryPath();
      cleanupOldRepositoriesInternal(repositoryRootPath, maxTotalSizeBytes, activeRepositoryPath);
    } catch (Exception e) {
      LOGGER.warn("Failed to clean up old JFR repositories for recording '{}' under '{}'", recordingName,
          repositoryRoot, e);
    }
  }

  @Nullable
  private Path resolveCurrentRepositoryPath() {
    if (!isDiagnosticCommandAvailable()) {
      return null;
    }
    try {
      Object result = _mBeanServer.invoke(
          _diagnosticCommandObjectName,
          JFR_CONFIGURE_COMMAND,
          new Object[]{new String[0]},
          new String[]{String[].class.getName()});
      if (result == null) {
        return null;
      }
      String output = result.toString();
      for (String line : output.split("\\R")) {
        String trimmedLine = line.trim();
        if (trimmedLine.startsWith("Repository path:")) {
          String repositoryPath = trimmedLine.substring("Repository path:".length()).trim();
          if (!repositoryPath.isEmpty()) {
            return Path.of(repositoryPath).toAbsolutePath().normalize();
          }
        }
      }
      return null;
    } catch (Exception e) {
      LOGGER.warn("Failed to resolve current JFR repository path from {}", JFR_CONFIGURE_COMMAND, e);
      return null;
    }
  }

  @VisibleForTesting
  static void cleanupOldRepositoriesInternal(Path repositoryRootPath, long maxTotalSizeBytes,
      @Nullable Path activeRepositoryPath) {
    if (!Files.isDirectory(repositoryRootPath) || maxTotalSizeBytes < 0) {
      return;
    }
    if (activeRepositoryPath != null && repositoryRootPath.equals(activeRepositoryPath)) {
      return;
    }
    try {
      List<PathWithMetadata> repositories = new ArrayList<>();
      try (java.util.stream.Stream<Path> children = Files.list(repositoryRootPath)) {
        children.filter(path -> activeRepositoryPath == null || !activeRepositoryPath.startsWith(path))
            .forEach(path -> repositories.add(
                new PathWithMetadata(path, computePathSize(path), readLastModifiedMillis(path))));
      }
      long totalSizeBytes = repositories.stream().mapToLong(pathWithMetadata -> pathWithMetadata._sizeBytes).sum();
      if (activeRepositoryPath != null) {
        totalSizeBytes += computePathSize(activeRepositoryPath);
      }
      if (totalSizeBytes <= maxTotalSizeBytes) {
        return;
      }
      repositories.sort((left, right) -> Long.compare(left._lastModifiedMillis, right._lastModifiedMillis));
      for (PathWithMetadata repository : repositories) {
        if (totalSizeBytes <= maxTotalSizeBytes) {
          break;
        }
        deletePathRecursively(repository._path);
        totalSizeBytes -= repository._sizeBytes;
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to clean old JFR repositories under '{}'", repositoryRootPath, e);
    }
  }

  private static long computePathSize(Path path) {
    if (!Files.exists(path)) {
      return 0;
    }
    SizeCollector sizeCollector = new SizeCollector();
    try {
      Files.walkFileTree(path, sizeCollector);
      return sizeCollector._sizeBytes;
    } catch (Exception e) {
      throw new RuntimeException("Failed to compute size for path '" + path + "'", e);
    }
  }

  private static long readLastModifiedMillis(Path path) {
    try {
      return Files.getLastModifiedTime(path).toMillis();
    } catch (Exception e) {
      return Long.MAX_VALUE;
    }
  }

  private static void deletePathRecursively(Path path) {
    if (!Files.exists(path)) {
      return;
    }
    try {
      Files.walkFileTree(path, new RecursiveDeleteVisitor());
    } catch (Exception e) {
      throw new RuntimeException("Failed to delete path '" + path + "'", e);
    }
  }

  @VisibleForTesting
  protected boolean isDiagnosticCommandAvailable() {
    return _mBeanServer != null && _diagnosticCommandObjectName != null
        && _mBeanServer.isRegistered(_diagnosticCommandObjectName);
  }

  private static ObjectName createDiagnosticCommandObjectName() {
    try {
      return new ObjectName(JFR_DIAGNOSTIC_MBEAN);
    } catch (MalformedObjectNameException e) {
      LOGGER.warn("Invalid JFR DiagnosticCommand MBean name '{}'. Continuous JFR control will be disabled",
          JFR_DIAGNOSTIC_MBEAN, e);
      return null;
    }
  }

  private static String toJfrTimeArgument(String durationText) {
    Duration duration = Duration.parse(durationText.toUpperCase(Locale.ENGLISH));
    if (duration.isZero()) {
      return "0s";
    }
    return duration.toMillis() + "ms";
  }

  private static class RecordingState {
    private Map<String, Object> _config;
    private int _referenceCount;
    @Nullable
    private ScheduledFuture<?> _cleanupTask;
    @Nullable
    private String _repositoryRoot;
    private long _repositoryMaxTotalSizeBytes;

    private RecordingState(Map<String, Object> config, int referenceCount) {
      _config = config;
      _referenceCount = referenceCount;
    }
  }

  private static final class PathWithMetadata {
    private final Path _path;
    private final long _sizeBytes;
    private final long _lastModifiedMillis;

    private PathWithMetadata(Path path, long sizeBytes, long lastModifiedMillis) {
      _path = path;
      _sizeBytes = sizeBytes;
      _lastModifiedMillis = lastModifiedMillis;
    }
  }

  private static final class SizeCollector extends SimpleFileVisitor<Path> {
    private long _sizeBytes;

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      _sizeBytes += attrs.size();
      return FileVisitResult.CONTINUE;
    }
  }

  private static final class RecursiveDeleteVisitor extends SimpleFileVisitor<Path> {
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
        throws java.io.IOException {
      Files.deleteIfExists(file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, @Nullable java.io.IOException exc)
        throws java.io.IOException {
      Files.deleteIfExists(dir);
      return FileVisitResult.CONTINUE;
    }
  }
}
