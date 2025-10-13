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
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ContinuousJfrStarter implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousJfrStarter.class);
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
  public static final boolean DEFAULT_DUMP_ON_EXIT = true;
  /// Key that controls the directory to store the recordings when dumped on exit.
  ///
  /// The default value is the current working directory.
  /// The filename will be 'recording-<timestamp>.jfr', where timestamp is in UTC and format is 'yyyy-MM-dd_HH-mm-ss'.
  public static final String DIRECTORY = "directory";
  /// Key that controls the maximum number of dumps to keep.
  ///
  /// If set, the directory will be cleaned up to keep only the most recent dumps.
  ///
  /// This is a Pinot feature, not a JFR feature and defaults to 10.
  /// If negative, no file will be removed.
  public static final String MAX_DUMPS = "maxDumps";
  public static final int DEFAULT_MAX_DUMPS = 10;

  /// Key that controls whether to buffer the recording to disk.
  /// If false, the recording will only be kept in memory.
  /// If true, the recording will be written to disk periodically. This may affect performance but increases the window
  /// of time that can be recorded.
  public static final String TO_DISK = "toDisk";
  /// Default value for the toDisk key.
  public static final boolean DEFAULT_TO_DISK = true;
  /// Key that controls the maximum size of the recording file.
  /// Once the file reaches this size, older events will be discarded.
  /// If both maxSize and maxAge are set, the recording will be discarded when either condition is met.
  ///
  /// This is only used if toDisk is true.
  /// The value is in bytes.
  /// The default value is 200MB.
  ///
  /// @see #MAX_AGE
  public static final String MAX_SIZE = "maxSize";
  public static final int DEFAULT_MAX_SIZE = 200 * 1024 * 1024;
  /// Key that controls the maximum age of the recording file.
  /// Once the file reaches this age, older events will be discarded.
  /// If both maxSize and maxAge are set, the recording will be discarded when either condition is met.
  ///
  /// This is only used if toDisk is true.
  /// The value is a duration string, as defined by the Duration class.
  /// The default value is 1 day (P1D).
  ///
  /// @see #MAX_SIZE
  public static final String MAX_AGE = "maxAge";
  public static final String DEFAULT_MAX_AGE = "P1D";

  /// A flag to track whether the JFR recording has been started.
  /// This is specially useful for testing and quickstarts, where servers, brokers and other components are executed
  /// in the same JVM.
  public static final ContinuousJfrStarter INSTANCE = new ContinuousJfrStarter();
  @GuardedBy("this")
  private boolean _running = false;
  @GuardedBy("this")
  private Map<String, Object> _currentConfig;
  @GuardedBy("this")
  private Recording _recording;
  @GuardedBy("this")
  private Thread _cleanupThread;

  @VisibleForTesting
  protected ContinuousJfrStarter() {
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

    synchronized (this) {
      Map<String, Object> newSubsetMap = subset.toMap();

      if (_currentConfig != null && _currentConfig.equals(newSubsetMap)) {
        // No change
        LOGGER.debug("JFR config change detected, but no actual change in config");
        return;
      }

      stopRecording();
      _currentConfig = newSubsetMap;
      startRecording(subset);
    }
  }

  public boolean isRunning() {
    return _running;
  }

  private void stopRecording() {
    if (!_running) {
      return;
    }
    assert _recording != null;
    LOGGER.debug("Stopping recording {}", _recording.getName());
    _recording.stop();
    _recording.close();

    if (_cleanupThread != null) {
      _cleanupThread.interrupt();
      try {
        _cleanupThread.join(5_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted while waiting for cleanup thread to stop");
      }
      _cleanupThread = null;
    }

    LOGGER.info("Stopped continuous JFR recording {}", _recording.getName());
    _recording = null;
    _running = false;
  }

  private void startRecording(PinotConfiguration subset) {
    if (!subset.getProperty(ENABLED, DEFAULT_ENABLED)) {
      LOGGER.info("Continuous JFR recording is disabled");
      return;
    }
    if (_running) {
      return;
    }

    _recording = createRecording(subset);
    _recording.setName(subset.getProperty(NAME, DEFAULT_NAME));

    _recording.setDumpOnExit(subset.getProperty(DUMP_ON_EXIT, DEFAULT_DUMP_ON_EXIT));

    prepareFileDumps(subset);

    try {
      boolean toDisk = subset.getProperty(TO_DISK, DEFAULT_TO_DISK);
      if (toDisk) {
        _recording.setToDisk(true);
        _recording.setMaxSize(subset.getProperty(MAX_SIZE, DEFAULT_MAX_SIZE));
        _recording.setMaxAge(Duration.parse(subset.getProperty(MAX_AGE, DEFAULT_MAX_AGE).toUpperCase(Locale.ENGLISH)));
      }
    } catch (DateTimeParseException e) {
      throw new RuntimeException("Failed to parse duration", e);
    }
    _recording.start();
    LOGGER.info("Started continuous JFR recording {} with configuration: {}", _recording.getName(), subset);
    _running = true;
  }

  @VisibleForTesting
  protected static Path getRecordingPath(Path parentDir, String name, Instant timestamp) {
    String filename = "recording-" + name + "-" + timestamp + ".jfr";
    return parentDir.resolve(filename);
  }

  private void prepareFileDumps(PinotConfiguration subset) {
    try {
      Path directory = Path.of(subset.getProperty(DIRECTORY, Paths.get(".").toString()));
      if (!directory.toFile().canWrite()) {
        throw new RuntimeException("Cannot write: " + directory);
      }

      Path recordingPath = getRecordingPath(directory, _recording.getName(), Instant.now());
      _recording.setDestination(recordingPath);

      int maxDumps = subset.getProperty(MAX_DUMPS, DEFAULT_MAX_DUMPS);
      if (maxDumps > 0) {
        _cleanupThread = createThread(() -> {
          while (!Thread.currentThread().isInterrupted()) {
            cleanUpDumps(directory, maxDumps, _recording.getName());
            try {
              Thread.sleep(Duration.ofHours(1).toMillis());
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        });
        _cleanupThread.start();
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new recording file", e);
    }
  }

  private Recording createRecording(PinotConfiguration subset) {
    String jfrConfName = subset.getProperty(CONFIGURATION, DEFAULT_CONFIGURATION);
    try {
      Configuration configuration = Configuration.getConfiguration(jfrConfName);
      return new Recording(configuration);
    } catch (ParseException e) {
      throw new RuntimeException("Failed to parse JFR configuration '" + jfrConfName + "'", e);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read JFR configuration '" + jfrConfName + "'", e);
    }
  }

  private Thread createThread(Runnable runnable) {
    Thread thread = new Thread(runnable);
    thread.setName("JFR-Dump-Cleanup");
    thread.setDaemon(true);
    return thread;
  }

  @VisibleForTesting
  protected static void cleanUpDumps(Path directory, int maxDumps, String recordingName) {
    if (maxDumps < 0) {
      LOGGER.debug("maxDumps is negative, no cleanup will be performed");
      return;
    }
    LOGGER.info("Cleaning up old JFR dumps in {} to keep at most {} dumps", directory, maxDumps);
    File[] files = directory.toFile()
        .listFiles((dir, name) -> name.startsWith("recording-" + recordingName) && name.endsWith(".jfr"));
    if (files == null) {
      return;
    }
    Arrays.sort(files, Comparator.comparing(File::getName).reversed());
    if (files.length <= maxDumps) {
      LOGGER.info("No cleanup needed, found {} dumps", files.length);
      return;
    }
    File[] filesToDelete = Arrays.copyOfRange(files, maxDumps, files.length);
    if (LOGGER.isInfoEnabled()) {
      String filesToDeleteName = Arrays.stream(filesToDelete)
          .map(File::getName)
          .collect(Collectors.joining(", "));
      LOGGER.info("Found {} dumps, going to delete the following older dumps {}", files.length, filesToDeleteName);
    }
    for (int i = maxDumps; i < files.length; i++) {
      boolean delete = files[i].delete();
      if (!delete) {
        LOGGER.warn("Failed to delete file: {}", files[i]);
      }
    }
  }
}
