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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


public class ContinuousJfrStarter {

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
  /// The filename will be 'recording-<timestamp>.jfr'.
  public static final String DIRECTORY = "directory";

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
  private static boolean _started = false;

  private ContinuousJfrStarter() {
  }

  public synchronized static void init(PinotConfiguration config) {
    PinotConfiguration subset = config.subset(CommonConstants.JFR);

    if (!subset.getProperty(ENABLED, DEFAULT_ENABLED)) {
      return;
    }
    if (_started) {
      return;
    }

    Path directory = Path.of(subset.getProperty(DIRECTORY, Paths.get(".").toString()));

    Recording recording;
    String jfrConfName = subset.getProperty(CONFIGURATION, DEFAULT_CONFIGURATION);
    try {
      Configuration configuration = Configuration.getConfiguration(jfrConfName);
      recording = new Recording(configuration);
    } catch (ParseException e) {
      throw new RuntimeException("Failed to parse JFR configuration '" + jfrConfName + "'", e);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read JFR configuration '" + jfrConfName + "'", e);
    }
    boolean dumpOnExit = subset.getProperty(DUMP_ON_EXIT, DEFAULT_DUMP_ON_EXIT);
    recording.setDumpOnExit(dumpOnExit);
    if (dumpOnExit) {
      try {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
        String filename = "recording-" + timestamp + ".jfr";
        Path recordingPath = directory.resolve(filename);
        boolean newFile = recordingPath.toFile().createNewFile();
        if (!newFile) {
          throw new RuntimeException("Failed to create new file: " + recordingPath);
        }
        recording.setDestination(recordingPath);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to create new recording file", e);
      }
    }

    try {
      recording.setName(subset.getProperty(NAME, DEFAULT_NAME));
      boolean toDisk = subset.getProperty(TO_DISK, DEFAULT_TO_DISK);
      if (toDisk) {
        recording.setToDisk(true);
        recording.setMaxSize(subset.getProperty(MAX_SIZE, DEFAULT_MAX_SIZE));
        recording.setMaxAge(Duration.parse(subset.getProperty(MAX_AGE, DEFAULT_MAX_AGE).toUpperCase(Locale.ENGLISH)));
      }
    } catch (DateTimeParseException e) {
      throw new RuntimeException("Failed to parse duration", e);
    }
    recording.start();
    _started = true;
  }
}
