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
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
  /// Key that controls the JFR repository directory path.
  ///
  /// The default value is the tmp directory of the system, as determined by the `java.io.tmpdir` system property.
  ///
  /// If set, this is applied via the JFR DiagnosticCommand MBean as `repositorypath`.
  public static final String DIRECTORY = "directory";
  /// Key that controls the JFR default dump directory path.
  /// If set, this is applied via the JFR DiagnosticCommand MBean as `dumppath`.
  public static final String DUMP_PATH = "dumpPath";

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

    synchronized (this) {
      Map<String, Object> newSubsetMap = subset.toMap();

      if (_currentConfig != null && _currentConfig.equals(newSubsetMap)) {
        // No change
        LOGGER.debug("JFR config change detected, but no actual change in config");
        return;
      }

      if (!stopRecording()) {
        LOGGER.warn("Failed to stop existing continuous JFR recording. Skipping config update");
        return;
      }
      if (startRecording(subset)) {
        _currentConfig = newSubsetMap;
      } else {
        LOGGER.warn("Failed to apply continuous JFR config update");
      }
    }
  }

  public boolean isRunning() {
    return _running;
  }

  private boolean stopRecording() {
    if (!_running) {
      return true;
    }
    assert _recordingName != null;
    if (!isDiagnosticCommandAvailable()) {
      LOGGER.warn("JFR DiagnosticCommand MBean is unavailable. Cannot stop continuous JFR recording '{}'",
          _recordingName);
      return false;
    }
    if (!executeDiagnosticCommand(JFR_STOP_COMMAND, "name=" + _recordingName)) {
      LOGGER.warn("Failed to stop continuous JFR recording '{}'", _recordingName);
      return false;
    }
    LOGGER.info("Stopped continuous JFR recording {}", _recordingName);
    _recordingName = null;
    _running = false;
    return true;
  }

  private boolean startRecording(PinotConfiguration subset) {
    if (!subset.getProperty(ENABLED, DEFAULT_ENABLED)) {
      LOGGER.info("Continuous JFR recording is disabled");
      return true;
    }
    if (_running) {
      return true;
    }
    if (!isDiagnosticCommandAvailable()) {
      LOGGER.warn("JFR DiagnosticCommand MBean is unavailable. Cannot start continuous JFR recording");
      return false;
    }

    String recordingName = subset.getProperty(NAME, DEFAULT_NAME);
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

    _recordingName = recordingName;
    LOGGER.info("Started continuous JFR recording {} with configuration: {}", recordingName, subset);
    _running = true;
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

    if (!configureArguments.isEmpty()) {
      return executeDiagnosticCommand(JFR_CONFIGURE_COMMAND, configureArguments.toArray(new String[0]));
    }
    return true;
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
}
