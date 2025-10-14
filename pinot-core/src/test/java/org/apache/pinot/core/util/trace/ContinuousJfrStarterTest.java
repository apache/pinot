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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import jdk.jfr.Recording;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ContinuousJfrStarterTest {

  private Recording _recording;
  private ContinuousJfrStarter _continuousJfrStarter;

  @BeforeMethod
  public void setUp() {
    _recording = Mockito.mock(Recording.class);
    _continuousJfrStarter = new ContinuousJfrStarter() {
      @Override
      protected Recording createRecording(PinotConfiguration subset) {
        return _recording;
      }
    };
  }

  @Test
  public void disabledByDefault() {
    _continuousJfrStarter.onChange(Set.of(), Map.of());

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be disabled")
        .isFalse();
    Mockito.verifyNoInteractions(_recording);
  }

  @Test
  public void canBeEnabled() {
    Map<String, String> config = Map.of("pinot.jfr.enabled", "true");
    _continuousJfrStarter.onChange(Set.of(), config);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be enabled")
        .isTrue();
    Mockito.verify(_recording).start();
  }

  @Test
  public void canBeTurnedOff() {
    // First start it
    Map<String, String> enabledConfig = Map.of("pinot.jfr.enabled", "true");
    _continuousJfrStarter.onChange(Set.of(), enabledConfig);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be enabled")
        .isTrue();
    Mockito.verify(_recording).start();

    // Then stop it
    Set<String> changed = Set.of("pinot.jfr.enabled");
    Map<String, String> disabledConfig = Map.of("pinot.jfr.enabled", "false");
    _continuousJfrStarter.onChange(changed, disabledConfig);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be disabled")
        .isFalse();
    Mockito.verify(_recording).stop();
  }

  @Test
  public void canBeTurnedOn() {
    // First setup with it off
    Map<String, String> disabledConfig = Map.of("pinot.jfr.enabled", "false");
    _continuousJfrStarter.onChange(Set.of(), disabledConfig);
    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be disabled")
        .isFalse();
    Mockito.verifyNoInteractions(_recording);

    // Then start it
    Set<String> changed = Set.of("pinot.jfr.enabled");
    Map<String, String> enabledConfig = Map.of("pinot.jfr.enabled", "true");
    _continuousJfrStarter.onChange(changed, enabledConfig);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be enabled")
        .isTrue();
    Mockito.verify(_recording).start();
  }

  @Test
  public void noOpWhenOtherPropChanges() {
    // First setup with it off
    Map<String, String> disabledConfig = Map.of("pinot.jfr.enabled", "false");
    _continuousJfrStarter.onChange(Set.of(), disabledConfig);
    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be disabled")
        .isFalse();
    Mockito.verifyNoInteractions(_recording);

    // Then call onChange with no relevant changes
    Set<String> changed = Set.of("some.other.config");
    Map<String, String> stillDisabledConfig = Map.of("pinot.jfr.enabled", "false", "some.other.config", "true");
    _continuousJfrStarter.onChange(changed, stillDisabledConfig);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should still be disabled")
        .isFalse();
    Mockito.verifyNoInteractions(_recording);
  }

  @Test
  public void noOpWhenNewConfigIsEqual() {
    // First setup with it off
    Map<String, String> disabledConfig = Map.of("pinot.jfr.enabled", "false");
    _continuousJfrStarter.onChange(Set.of(), disabledConfig);
    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be disabled")
        .isFalse();
    Mockito.verifyNoInteractions(_recording);

    // Then call onChange with the same config
    Set<String> changed = Set.of("pinot.jfr.enabled");
    Map<String, String> stillDisabledConfig = Map.of("pinot.jfr.enabled", "false");
    _continuousJfrStarter.onChange(changed, stillDisabledConfig);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should still be disabled")
        .isFalse();
    Mockito.verifyNoInteractions(_recording);
  }

  @Test
  public void cleanUpThreadDeletesFiles()
      throws IOException {
    Path tempDirectory = Files.createTempDirectory("jfr-test-");
    int maxDumps = 3;
    long now = ZonedDateTime.of(2025, 10, 13, 12, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
    try {
      long[] dates = IntStream.range(0, maxDumps * 2)
          .mapToLong(i -> now - i * 3600_000L)
          .sorted()
          .toArray();
      for (long creationDate : dates) {
        Path path = ContinuousJfrStarter.getRecordingPath(tempDirectory, "test", Instant.ofEpochMilli(creationDate));
        File file = path.toFile();
        Assertions.assertThat(file.createNewFile())
            .describedAs("Should be able to create a file in the temp directory")
            .isTrue();
        Assertions.assertThat(file.setLastModified(creationDate))
            .describedAs("Should be able to set the last modified time")
            .isTrue();
      }

      // Verify that we have 2 * maxDumps files
      try (var files = Files.list(tempDirectory)) {
        Assertions.assertThat(files.count())
            .describedAs("Should have 2 * maxDumps files in the temp directory")
            .isEqualTo(maxDumps * 2);
      }

      // Run the cleanup
      ContinuousJfrStarter.cleanUpDumps(tempDirectory, maxDumps, "test");

      // Verify that we have maxDumps files and only the newest ones are kept
      try (var files = Files.list(tempDirectory)) {
        var remainingFiles = files.collect(Collectors.toSet());
        Assertions.assertThat(remainingFiles)
            .describedAs("Should have maxDumps files in the temp directory")
            .hasSize(maxDumps);
        for (int i = 0; i < maxDumps; i++) {
          long creationDate = dates[dates.length - 1 - i];
          Instant timestamp = Instant.ofEpochMilli(creationDate);
          Path expectedPath = ContinuousJfrStarter.getRecordingPath(tempDirectory, "test", timestamp);
          Assertions.assertThat(remainingFiles)
              .describedAs("Should contain the expected file: %s", expectedPath)
              .contains(expectedPath);
        }
      }

      // Clean all files
      ContinuousJfrStarter.cleanUpDumps(tempDirectory, 0, "test");
      try (var files = Files.list(tempDirectory)) {
        Assertions.assertThat(files.count())
            .describedAs("Should have no files in the temp directory")
            .isEqualTo(0);
      }
    } finally {
      FileUtils.deleteDirectory(tempDirectory.toFile());
    }
  }
}
