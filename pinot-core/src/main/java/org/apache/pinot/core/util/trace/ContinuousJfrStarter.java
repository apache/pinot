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
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import jdk.jfr.RecordingState;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Starts, stops and retunes a continuous JFR recording from the `pinot.jfr.*` cluster configs, without a restart.
///
/// The JVM owns the JFR options that are global to the process: the repository path, `preserve-repository`, the
/// dump path and the chunk size. Set them with `-XX:FlightRecorderOptions`, for example
/// `-XX:FlightRecorderOptions:repository=/var/pinot/jfr,preserve-repository=true`. This class owns only the options
/// of one recording.
///
/// Each Pinot component registers its own instance with its instance ID. A component records when
/// `pinot.jfr.enabled` is `true` and `pinot.jfr.instanceRegex` matches its full instance ID. Components that run in
/// the same JVM (quickstarts and tests) share one recording for each recording name.
///
/// If the JVM starts with `-XX:StartFlightRecording`, this class does nothing, because the JVM flags own the
/// recording.
public class ContinuousJfrStarter implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousJfrStarter.class);
  private static final String START_FLIGHT_RECORDING_ARGUMENT = "-XX:StartFlightRecording";

  /// Key that controls whether to enable continuous JFR recording.
  public static final String ENABLED = "enabled";
  /// Default value for the enabled key.
  public static final boolean DEFAULT_ENABLED = false;

  /// Key that selects the instances that record. The value is a regular expression that must match the full
  /// instance ID, for example `Server_.*` or `(Broker|Controller)_.*`.
  ///
  /// If the key is not set, all instances record. If the value is not a valid regular expression, the instance
  /// logs an error and keeps its current state.
  public static final String INSTANCE_REGEX = "instanceRegex";

  /// Key that controls the JFR configuration to use. Valid values are 'default' or 'profile', or the path of a
  /// custom `.jfc` file. See the JFR documentation for more information.
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
  public static final boolean DEFAULT_DUMP_ON_EXIT = false;

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

  /// Keys that older versions read. The JVM owns these options now, so this class ignores them and logs the
  /// replacement for each one.
  private static final Map<String, String> REMOVED_KEYS = Map.of(
      "directory", "-XX:FlightRecorderOptions:repository=<path>",
      "dumpPath", "-XX:FlightRecorderOptions:dumppath=<path>",
      "maxDumps", "a cleanup of the repository directory outside the JVM",
      "preserveRepository", "-XX:FlightRecorderOptions:preserve-repository=true",
      "repositoryMaxTotalSize", "a cleanup of the repository directory outside the JVM");

  private static final Object GLOBAL_RECORDING_LOCK = new Object();
  /// The recordings that this class runs, by recording name. Several instances in the same JVM share an entry.
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private static final Map<String, SharedRecording> GLOBAL_RECORDINGS = new HashMap<>();
  /// The last stopped recording for each recording name.
  ///
  /// `Recording.stop()` keeps the recorded chunks in the repository, but `Recording.close()` deletes them. This class
  /// keeps the last stopped recording open, so its data survives a disable or a restart of the recording. It closes
  /// the recording before it, so the disk usage stays bounded.
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private static final Map<String, Recording> STOPPED_RECORDINGS = new HashMap<>();

  private final String _instanceId;
  private final boolean _recordingStartedByJvmArgument;
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  @Nullable
  private Map<String, Object> _currentConfig;
  /// The name of the recording this instance holds a reference to, or null if it does not record.
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  @Nullable
  private String _recordingName;
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean _jvmArgumentWarningLogged;
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean _closed;

  public ContinuousJfrStarter(String instanceId) {
    this(instanceId, ManagementFactory.getRuntimeMXBean().getInputArguments());
  }

  @VisibleForTesting
  ContinuousJfrStarter(String instanceId, List<String> jvmArguments) {
    _instanceId = instanceId;
    _recordingStartedByJvmArgument = jvmArguments.stream()
        .anyMatch(argument -> argument.startsWith(START_FLIGHT_RECORDING_ARGUMENT));
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    boolean jfrChanged = changedConfigs.stream()
        .anyMatch(changedConfig -> changedConfig.startsWith(CommonConstants.JFR));
    PinotConfiguration subset = new PinotConfiguration(clusterConfigs).subset(CommonConstants.JFR);

    synchronized (GLOBAL_RECORDING_LOCK) {
      if (_closed) {
        return;
      }
      if (!jfrChanged && _currentConfig != null) {
        LOGGER.debug("ChangedConfigs: {} does not contain any JFR config. Skipping updates", changedConfigs);
        return;
      }
      Map<String, Object> newSubsetMap = subset.toMap();
      if (newSubsetMap.equals(_currentConfig)) {
        LOGGER.debug("JFR config change detected, but no actual change in config");
        return;
      }
      if (applyConfig(subset)) {
        _currentConfig = newSubsetMap;
      } else {
        LOGGER.warn("Failed to apply continuous JFR config update on instance {}", _instanceId);
      }
    }
  }

  /// Returns whether this instance holds a reference to a recording that is running.
  public boolean isRunning() {
    synchronized (GLOBAL_RECORDING_LOCK) {
      return _recordingName != null
          && GLOBAL_RECORDINGS.get(_recordingName)._recording.getState() == RecordingState.RUNNING;
    }
  }

  /// Gives back the reference of this instance to its recording, and ignores later config changes. The component that
  /// registered this instance calls this method when it stops. Without it, a component that restarts in the same JVM
  /// (for example in tests) keeps a reference to the recording, and `enabled=false` no longer stops it.
  public void close() {
    synchronized (GLOBAL_RECORDING_LOCK) {
      if (_closed) {
        return;
      }
      _closed = true;
      releaseRecordingReference();
      _currentConfig = null;
    }
  }

  /// Closes all the recordings that this class started, in all instances, and forgets them.
  @VisibleForTesting
  static void resetGlobalStateForTesting() {
    synchronized (GLOBAL_RECORDING_LOCK) {
      GLOBAL_RECORDINGS.values().forEach(sharedRecording -> sharedRecording._recording.close());
      GLOBAL_RECORDINGS.clear();
      STOPPED_RECORDINGS.values().forEach(Recording::close);
      STOPPED_RECORDINGS.clear();
    }
  }

  /// Returns the recording that this instance holds a reference to, or null if it does not record.
  @VisibleForTesting
  @Nullable
  Recording getRecording() {
    synchronized (GLOBAL_RECORDING_LOCK) {
      return _recordingName != null ? GLOBAL_RECORDINGS.get(_recordingName)._recording : null;
    }
  }

  /// Returns the last stopped recording with the given name, or null if there is none.
  @VisibleForTesting
  @Nullable
  static Recording getStoppedRecording(String recordingName) {
    synchronized (GLOBAL_RECORDING_LOCK) {
      return STOPPED_RECORDINGS.get(recordingName);
    }
  }

  /// Applies the config to this instance. Returns false and keeps the current state if the config is not valid.
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean applyConfig(PinotConfiguration subset) {
    for (Map.Entry<String, String> removedKey : REMOVED_KEYS.entrySet()) {
      if (subset.containsKey(removedKey.getKey())) {
        LOGGER.warn("Ignoring continuous JFR config '{}.{}'. Use {} instead", CommonConstants.JFR,
            removedKey.getKey(), removedKey.getValue());
      }
    }
    Boolean shouldRecord = shouldRecord(subset);
    if (shouldRecord == null) {
      return false;
    }
    if (!shouldRecord) {
      releaseRecordingReference();
      return true;
    }
    RecordingSpec spec;
    try {
      spec = RecordingSpec.from(subset);
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid continuous JFR config on instance {}", _instanceId, e);
      return false;
    }
    if (_recordingName != null && !_recordingName.equals(spec._name)) {
      releaseRecordingReference();
    }
    return acquireRecordingReference(spec);
  }

  /// Returns whether this instance must record, or null if the instance regex is not valid.
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  @Nullable
  private Boolean shouldRecord(PinotConfiguration subset) {
    if (!subset.getProperty(ENABLED, DEFAULT_ENABLED)) {
      return false;
    }
    if (_recordingStartedByJvmArgument) {
      if (!_jvmArgumentWarningLogged) {
        LOGGER.warn("Ignoring continuous JFR cluster configs on instance {}, because the JVM starts a recording with "
            + "{}", _instanceId, START_FLIGHT_RECORDING_ARGUMENT);
        _jvmArgumentWarningLogged = true;
      }
      return false;
    }
    String instanceRegex = subset.getProperty(INSTANCE_REGEX);
    if (instanceRegex == null || instanceRegex.isEmpty()) {
      return true;
    }
    try {
      return Pattern.compile(instanceRegex).matcher(_instanceId).matches();
    } catch (PatternSyntaxException e) {
      LOGGER.error("Invalid continuous JFR config '{}.{}': '{}'. Keeping the current recording state on instance {}",
          CommonConstants.JFR, INSTANCE_REGEX, instanceRegex, _instanceId, e);
      return null;
    }
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean acquireRecordingReference(RecordingSpec spec) {
    SharedRecording sharedRecording = GLOBAL_RECORDINGS.get(spec._name);
    if (sharedRecording == null) {
      Recording recording = startRecording(spec);
      if (recording == null) {
        return false;
      }
      GLOBAL_RECORDINGS.put(spec._name, new SharedRecording(recording, spec));
      _recordingName = spec._name;
      return true;
    }
    if (isStopped(sharedRecording._recording)) {
      if (!restartRecording(sharedRecording, spec)) {
        return false;
      }
    } else if (!sharedRecording._spec.equals(spec) && !updateRecording(sharedRecording, spec)) {
      return false;
    }
    if (!spec._name.equals(_recordingName)) {
      sharedRecording._referenceCount++;
      _recordingName = spec._name;
    }
    return true;
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private void releaseRecordingReference() {
    if (_recordingName == null) {
      return;
    }
    SharedRecording sharedRecording = GLOBAL_RECORDINGS.get(_recordingName);
    if (sharedRecording._referenceCount > 1) {
      sharedRecording._referenceCount--;
    } else {
      stopRecording(_recordingName, sharedRecording._recording);
      GLOBAL_RECORDINGS.remove(_recordingName);
    }
    _recordingName = null;
  }

  /// Applies a new spec to a running recording. JFR can change all the options of a running recording, except the
  /// `toDisk` option. For that option, this method starts a new recording and stops the old one.
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean updateRecording(SharedRecording sharedRecording, RecordingSpec spec) {
    if (sharedRecording._spec._toDisk != spec._toDisk) {
      Recording recording = startRecording(spec);
      if (recording == null) {
        return false;
      }
      stopRecording(spec._name, sharedRecording._recording);
      sharedRecording._recording = recording;
    } else {
      try {
        applyOptions(sharedRecording._recording, spec);
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to update continuous JFR recording '{}'", spec._name, e);
        return false;
      }
      LOGGER.info("Updated continuous JFR recording '{}' to {}", spec._name, spec);
    }
    sharedRecording._spec = spec;
    return true;
  }

  /// Returns whether the recording stopped without this class, for example with `jcmd <pid> JFR.stop`.
  private static boolean isStopped(Recording recording) {
    RecordingState state = recording.getState();
    return state == RecordingState.STOPPED || state == RecordingState.CLOSED;
  }

  /// Replaces a recording that stopped without this class with a new one. JFR cannot start a stopped or closed
  /// recording again, and rejects any change to a closed one.
  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private boolean restartRecording(SharedRecording sharedRecording, RecordingSpec spec) {
    LOGGER.warn("Continuous JFR recording '{}' is {}, but Pinot did not stop it. Starting a new recording", spec._name,
        sharedRecording._recording.getState());
    Recording recording = startRecording(spec);
    if (recording == null) {
      return false;
    }
    stopRecording(spec._name, sharedRecording._recording);
    sharedRecording._recording = recording;
    sharedRecording._spec = spec;
    return true;
  }

  @Nullable
  private static Recording startRecording(RecordingSpec spec) {
    Recording recording = null;
    try {
      recording = new Recording();
      recording.setName(spec._name);
      recording.setToDisk(spec._toDisk);
      applyOptions(recording, spec);
      recording.start();
    } catch (RuntimeException e) {
      LOGGER.warn("Failed to start continuous JFR recording '{}'", spec._name, e);
      if (recording != null) {
        recording.close();
      }
      return null;
    }
    LOGGER.info("Started continuous JFR recording '{}' with {}", spec._name, spec);
    return recording;
  }

  private static void applyOptions(Recording recording, RecordingSpec spec) {
    recording.setSettings(spec._settings);
    recording.setDumpOnExit(spec._dumpOnExit);
    if (spec._toDisk) {
      recording.setMaxSize(spec._maxSizeBytes);
      recording.setMaxAge(spec._maxAge);
    }
  }

  @GuardedBy("GLOBAL_RECORDING_LOCK")
  private static void stopRecording(String recordingName, Recording recording) {
    try {
      recording.stop();
    } catch (IllegalStateException e) {
      // Another tool, for example jcmd, stopped or closed the recording before.
      LOGGER.debug("Continuous JFR recording '{}' was not running", recordingName, e);
    }
    Recording previous = STOPPED_RECORDINGS.put(recordingName, recording);
    if (previous != null) {
      previous.close();
    }
    LOGGER.info("Stopped continuous JFR recording '{}'", recordingName);
  }

  private static class SharedRecording {
    private Recording _recording;
    private RecordingSpec _spec;
    private int _referenceCount = 1;

    private SharedRecording(Recording recording, RecordingSpec spec) {
      _recording = recording;
      _spec = spec;
    }
  }

  /// The options of one recording, read from the `pinot.jfr.*` cluster configs.
  private static final class RecordingSpec {
    private final String _name;
    private final String _configuration;
    private final Map<String, String> _settings;
    private final boolean _dumpOnExit;
    private final boolean _toDisk;
    private final long _maxSizeBytes;
    private final Duration _maxAge;

    private RecordingSpec(String name, String configuration, Map<String, String> settings, boolean dumpOnExit,
        boolean toDisk, long maxSizeBytes, Duration maxAge) {
      _name = name;
      _configuration = configuration;
      _settings = settings;
      _dumpOnExit = dumpOnExit;
      _toDisk = toDisk;
      _maxSizeBytes = maxSizeBytes;
      _maxAge = maxAge;
    }

    /// Reads the spec from the config subset. Throws `IllegalArgumentException` if a value is not valid.
    private static RecordingSpec from(PinotConfiguration subset) {
      String name = subset.getProperty(NAME, DEFAULT_NAME);
      String configuration = subset.getProperty(CONFIGURATION, DEFAULT_CONFIGURATION);
      String maxSize = subset.getProperty(MAX_SIZE, DEFAULT_MAX_SIZE);
      long maxSizeBytes;
      try {
        maxSizeBytes = DataSizeUtils.toBytes(maxSize);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid value for '" + MAX_SIZE + "': '" + maxSize + "'", e);
      }
      String maxAge = subset.getProperty(MAX_AGE, DEFAULT_MAX_AGE);
      Duration maxAgeDuration;
      try {
        maxAgeDuration = Duration.parse(maxAge.toUpperCase(Locale.ENGLISH));
      } catch (DateTimeParseException e) {
        throw new IllegalArgumentException("Invalid value for '" + MAX_AGE + "': '" + maxAge + "'", e);
      }
      return new RecordingSpec(name, configuration, loadSettings(configuration),
          subset.getProperty(DUMP_ON_EXIT, DEFAULT_DUMP_ON_EXIT), subset.getProperty(TO_DISK, DEFAULT_TO_DISK),
          maxSizeBytes, maxAgeDuration);
    }

    /// Loads the settings of a predefined JFR configuration, or of a `.jfc` file if the value is a file path.
    private static Map<String, String> loadSettings(String configuration) {
      try {
        Path path = toFilePath(configuration);
        return path != null ? Configuration.create(path).getSettings()
            : Configuration.getConfiguration(configuration).getSettings();
      } catch (IOException | ParseException e) {
        throw new IllegalArgumentException("Invalid value for '" + CONFIGURATION + "': '" + configuration + "'", e);
      }
    }

    @Nullable
    private static Path toFilePath(String configuration) {
      try {
        Path path = Path.of(configuration);
        return Files.isRegularFile(path) ? path : null;
      } catch (InvalidPathException e) {
        return null;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RecordingSpec)) {
        return false;
      }
      RecordingSpec that = (RecordingSpec) o;
      return _dumpOnExit == that._dumpOnExit && _toDisk == that._toDisk && _maxSizeBytes == that._maxSizeBytes
          && _name.equals(that._name) && _settings.equals(that._settings) && _maxAge.equals(that._maxAge);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_name, _settings, _dumpOnExit, _toDisk, _maxSizeBytes, _maxAge);
    }

    @Override
    public String toString() {
      return "{configuration=" + _configuration + ", dumpOnExit=" + _dumpOnExit + ", toDisk=" + _toDisk
          + ", maxSize=" + _maxSizeBytes + ", maxAge=" + _maxAge + "}";
    }
  }
}
