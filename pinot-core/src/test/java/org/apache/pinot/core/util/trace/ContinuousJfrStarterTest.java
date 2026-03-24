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

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.assertj.core.api.Assertions;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ContinuousJfrStarterTest {
  private static final long DEFAULT_MAX_SIZE_BYTES = DataSizeUtils.toBytes(ContinuousJfrStarter.DEFAULT_MAX_SIZE);
  private static final long DEFAULT_MAX_AGE_MILLIS = 7L * 24 * 60 * 60 * 1000;

  private TestContinuousJfrStarter _continuousJfrStarter;

  @BeforeMethod
  public void setUp() {
    ContinuousJfrStarter.resetGlobalStateForTesting();
    _continuousJfrStarter = new TestContinuousJfrStarter();
  }

  @Test
  public void disabledByDefault() {
    _continuousJfrStarter.onChange(Set.of(), Map.of());

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be disabled")
        .isFalse();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).isEmpty();
  }

  @Test
  public void canBeEnabled() {
    Map<String, String> config = Map.of("pinot.jfr.enabled", "true");
    _continuousJfrStarter.onChange(Set.of(), config);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be enabled")
        .isTrue();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms");
  }

  @Test
  public void canBeTurnedOff() {
    // First start it
    Map<String, String> enabledConfig = Map.of("pinot.jfr.enabled", "true");
    _continuousJfrStarter.onChange(Set.of(), enabledConfig);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be enabled")
        .isTrue();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).hasSize(1);

    // Then stop it
    Set<String> changed = Set.of("pinot.jfr.enabled");
    Map<String, String> disabledConfig = Map.of("pinot.jfr.enabled", "false");
    _continuousJfrStarter.onChange(changed, disabledConfig);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be disabled")
        .isFalse();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms",
        "jfrStop name=pinot-continuous");
  }

  @Test
  public void canBeTurnedOn() {
    // First setup with it off
    Map<String, String> disabledConfig = Map.of("pinot.jfr.enabled", "false");
    _continuousJfrStarter.onChange(Set.of(), disabledConfig);
    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be disabled")
        .isFalse();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).isEmpty();

    // Then start it
    Set<String> changed = Set.of("pinot.jfr.enabled");
    Map<String, String> enabledConfig = Map.of("pinot.jfr.enabled", "true");
    _continuousJfrStarter.onChange(changed, enabledConfig);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be enabled")
        .isTrue();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms");
  }

  @Test
  public void noOpWhenOtherPropChanges() {
    // First setup with it off
    Map<String, String> disabledConfig = Map.of("pinot.jfr.enabled", "false");
    _continuousJfrStarter.onChange(Set.of(), disabledConfig);
    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be disabled")
        .isFalse();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).isEmpty();

    // Then call onChange with no relevant changes
    Set<String> changed = Set.of("some.other.config");
    Map<String, String> stillDisabledConfig = Map.of("pinot.jfr.enabled", "false", "some.other.config", "true");
    _continuousJfrStarter.onChange(changed, stillDisabledConfig);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should still be disabled")
        .isFalse();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).isEmpty();
  }

  @Test
  public void noOpWhenNewConfigIsEqual() {
    // First setup with it off
    Map<String, String> disabledConfig = Map.of("pinot.jfr.enabled", "false");
    _continuousJfrStarter.onChange(Set.of(), disabledConfig);
    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should be disabled")
        .isFalse();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).isEmpty();

    // Then call onChange with the same config
    Set<String> changed = Set.of("pinot.jfr.enabled");
    Map<String, String> stillDisabledConfig = Map.of("pinot.jfr.enabled", "false");
    _continuousJfrStarter.onChange(changed, stillDisabledConfig);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should still be disabled")
        .isFalse();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).isEmpty();
  }

  @Test
  public void restartsWhenConfigurationChanges() {
    Map<String, String> config = Map.of(
        "pinot.jfr.enabled", "true",
        "pinot.jfr.maxAge", "PT2H");
    _continuousJfrStarter.onChange(Set.of(), config);

    Set<String> changed = Set.of("pinot.jfr.maxAge");
    Map<String, String> updatedConfig = Map.of(
        "pinot.jfr.enabled", "true",
        "pinot.jfr.maxAge", "PT1H");
    _continuousJfrStarter.onChange(changed, updatedConfig);

    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=7200000ms",
        "jfrStop name=pinot-continuous",
        "jfrStart name=pinot-continuous settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=3600000ms");
  }

  @Test
  public void configuresRepositoryAndDumpPathsViaMBean() {
    Map<String, String> config = Map.of(
        "pinot.jfr.enabled", "true",
        "pinot.jfr.directory", "/var/log/pinot/jfr-repository",
        "pinot.jfr.dumpPath", "/var/log/pinot/jfr-dumps");
    _continuousJfrStarter.onChange(Set.of(), config);

    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).containsExactly(
        "jfrConfigure repositorypath=/var/log/pinot/jfr-repository dumppath=/var/log/pinot/jfr-dumps "
            + "preserve-repository=true",
        "jfrStart name=pinot-continuous settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms");
  }

  @Test
  public void doesNotFailWhenMBeanIsUnavailable() {
    _continuousJfrStarter.setMBeanAvailable(false);
    Map<String, String> config = Map.of("pinot.jfr.enabled", "true");
    _continuousJfrStarter.onChange(Set.of(), config);

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Recording should remain disabled when MBean is unavailable")
        .isFalse();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).isEmpty();
  }

  @Test
  public void keepsRunningWhenStopCommandFails() {
    _continuousJfrStarter.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));
    _continuousJfrStarter.failCommand("jfrStop");

    _continuousJfrStarter.onChange(Set.of("pinot.jfr.enabled"), Map.of("pinot.jfr.enabled", "false"));

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Starter should keep running state when stop fails")
        .isTrue();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms",
        "jfrStop name=pinot-continuous");
  }

  @Test
  public void keepsRunningWhenMBeanBecomesUnavailableOnStop() {
    _continuousJfrStarter.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));
    _continuousJfrStarter.setMBeanAvailable(false);

    _continuousJfrStarter.onChange(Set.of("pinot.jfr.enabled"), Map.of("pinot.jfr.enabled", "false"));

    Assertions.assertThat(_continuousJfrStarter.isRunning())
        .describedAs("Starter should keep running state when MBean is unavailable for stop")
        .isTrue();
    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms");
  }

  @Test
  public void supportsHumanReadableMaxSize() {
    Map<String, String> config = Map.of(
        "pinot.jfr.enabled", "true",
        "pinot.jfr.maxSize", "512MB");
    _continuousJfrStarter.onChange(Set.of(), config);

    Assertions.assertThat(_continuousJfrStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous settings=default dumponexit=false disk=true maxsize=536870912 maxage="
            + DEFAULT_MAX_AGE_MILLIS + "ms");
  }

  @Test
  public void coordinatesMultipleStartersInSameJvm() {
    TestContinuousJfrStarter firstStarter = new TestContinuousJfrStarter();
    TestContinuousJfrStarter secondStarter = new TestContinuousJfrStarter();
    Map<String, String> enabledConfig = Map.of("pinot.jfr.enabled", "true");
    Map<String, String> disabledConfig = Map.of("pinot.jfr.enabled", "false");

    firstStarter.onChange(Set.of(), enabledConfig);
    secondStarter.onChange(Set.of(), enabledConfig);

    Assertions.assertThat(firstStarter.isRunning()).isTrue();
    Assertions.assertThat(secondStarter.isRunning()).isTrue();
    Assertions.assertThat(firstStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms");
    Assertions.assertThat(secondStarter.getExecutedCommands()).isEmpty();

    firstStarter.onChange(Set.of("pinot.jfr.enabled"), disabledConfig);
    Assertions.assertThat(firstStarter.isRunning()).isFalse();
    Assertions.assertThat(secondStarter.isRunning()).isTrue();
    Assertions.assertThat(firstStarter.getExecutedCommands()).hasSize(1);

    secondStarter.onChange(Set.of("pinot.jfr.enabled"), disabledConfig);
    Assertions.assertThat(secondStarter.isRunning()).isFalse();
    Assertions.assertThat(secondStarter.getExecutedCommands()).containsExactly("jfrStop name=pinot-continuous");
  }

  @Test
  public void coordinatesPerRecordingName() {
    TestContinuousJfrStarter firstStarter = new TestContinuousJfrStarter();
    TestContinuousJfrStarter secondStarter = new TestContinuousJfrStarter();
    Map<String, String> enabledFirstConfig = Map.of("pinot.jfr.enabled", "true", "pinot.jfr.name",
        "pinot-continuous-a");
    Map<String, String> enabledSecondConfig = Map.of("pinot.jfr.enabled", "true", "pinot.jfr.name",
        "pinot-continuous-b");
    Map<String, String> disabledFirstConfig =
        Map.of("pinot.jfr.enabled", "false", "pinot.jfr.name", "pinot-continuous-a");
    Map<String, String> disabledSecondConfig =
        Map.of("pinot.jfr.enabled", "false", "pinot.jfr.name", "pinot-continuous-b");

    firstStarter.onChange(Set.of(), enabledFirstConfig);
    secondStarter.onChange(Set.of(), enabledSecondConfig);

    Assertions.assertThat(firstStarter.isRunning()).isTrue();
    Assertions.assertThat(secondStarter.isRunning()).isTrue();
    Assertions.assertThat(firstStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous-a settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms");
    Assertions.assertThat(secondStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous-b settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms");

    firstStarter.onChange(Set.of("pinot.jfr.enabled"), disabledFirstConfig);
    Assertions.assertThat(firstStarter.isRunning()).isFalse();
    Assertions.assertThat(secondStarter.isRunning()).isTrue();
    Assertions.assertThat(firstStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous-a settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms",
        "jfrStop name=pinot-continuous-a");

    secondStarter.onChange(Set.of("pinot.jfr.enabled"), disabledSecondConfig);
    Assertions.assertThat(secondStarter.isRunning()).isFalse();
    Assertions.assertThat(secondStarter.getExecutedCommands()).containsExactly(
        "jfrStart name=pinot-continuous-b settings=default dumponexit=false disk=true maxsize=" + DEFAULT_MAX_SIZE_BYTES
            + " maxage=" + DEFAULT_MAX_AGE_MILLIS + "ms",
        "jfrStop name=pinot-continuous-b");
  }

  @Test
  public void integrationTestWithRealDiagnosticCommandMBean() {
    ContinuousJfrStarter starter = new ContinuousJfrStarter();
    if (!starter.isDiagnosticCommandAvailable()) {
      throw new SkipException("JFR DiagnosticCommand MBean is not available in this runtime");
    }

    String recordingName = "pinot-continuous-it-" + System.currentTimeMillis();
    Map<String, String> enabledConfig = Map.of(
        "pinot.jfr.enabled", "true",
        "pinot.jfr.name", recordingName,
        "pinot.jfr.toDisk", "false");
    Map<String, String> disabledConfig = Map.of(
        "pinot.jfr.enabled", "false",
        "pinot.jfr.name", recordingName,
        "pinot.jfr.toDisk", "false");

    try {
      starter.onChange(Set.of(), enabledConfig);
      Assertions.assertThat(starter.isRunning()).isTrue();
      Assertions.assertThat(waitForRecordingPresence(recordingName, true, 10, 100))
          .describedAs("Recording should be visible via JFR.check")
          .isTrue();

      starter.onChange(Set.of("pinot.jfr.enabled"), disabledConfig);
      Assertions.assertThat(starter.isRunning()).isFalse();
      Assertions.assertThat(waitForRecordingPresence(recordingName, false, 10, 100))
          .describedAs("Recording should be absent via JFR.check after disable")
          .isTrue();
    } finally {
      if (starter.isRunning()) {
        starter.onChange(Set.of("pinot.jfr.enabled"), disabledConfig);
      }
    }
  }

  @Test
  public void removesOldRepositoriesWhenTotalSizeExceedsLimit() throws Exception {
    Path root = Files.createTempDirectory("pinot-jfr-repo-root");
    try {
      Path oldest = createRepositoryWithSize(root.resolve("repo-oldest"), 8);
      Path newer = createRepositoryWithSize(root.resolve("repo-newer"), 8);
      Path active = createRepositoryWithSize(root.resolve("repo-active"), 8);
      Files.setLastModifiedTime(oldest, FileTime.fromMillis(1000));
      Files.setLastModifiedTime(newer, FileTime.fromMillis(2000));
      Files.setLastModifiedTime(active, FileTime.fromMillis(3000));

      ContinuousJfrStarter.cleanupOldRepositoriesInternal(root, 16, active);

      Assertions.assertThat(Files.exists(oldest)).isFalse();
      Assertions.assertThat(Files.exists(newer)).isTrue();
      Assertions.assertThat(Files.exists(active)).isTrue();
    } finally {
      ContinuousJfrStarter.cleanupOldRepositoriesInternal(root, 0, null);
      Files.deleteIfExists(root);
    }
  }

  @Test
  public void keepsActiveRepositoryEvenWhenLargerThanLimit() throws Exception {
    Path root = Files.createTempDirectory("pinot-jfr-repo-root");
    try {
      Path old = createRepositoryWithSize(root.resolve("repo-old"), 8);
      Path active = createRepositoryWithSize(root.resolve("repo-active"), 24);
      Files.setLastModifiedTime(old, FileTime.fromMillis(1000));
      Files.setLastModifiedTime(active, FileTime.fromMillis(2000));

      ContinuousJfrStarter.cleanupOldRepositoriesInternal(root, 20, active);

      Assertions.assertThat(Files.exists(old)).isFalse();
      Assertions.assertThat(Files.exists(active)).isTrue();
    } finally {
      ContinuousJfrStarter.cleanupOldRepositoriesInternal(root, 0, null);
      Files.deleteIfExists(root);
    }
  }

  private static Path createRepositoryWithSize(Path repositoryPath, int bytes) throws Exception {
    Files.createDirectories(repositoryPath);
    Files.write(repositoryPath.resolve("chunk.jfr"), new byte[bytes]);
    return repositoryPath;
  }

  private static boolean waitForRecordingPresence(String recordingName, boolean expectedPresent, int maxAttempts,
      long delayMs) {
    for (int i = 0; i < maxAttempts; i++) {
      boolean present = isRecordingPresent(recordingName);
      if (present == expectedPresent) {
        return true;
      }
      try {
        Thread.sleep(delayMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  private static boolean isRecordingPresent(String recordingName) {
    try {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      ObjectName objectName = new ObjectName("com.sun.management:type=DiagnosticCommand");
      Object result = mBeanServer.invoke(objectName, "jfrCheck", new Object[]{new String[0]},
          new String[]{String[].class.getName()});
      return result != null && result.toString().contains(recordingName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to query JFR.check via DiagnosticCommand MBean", e);
    }
  }

  private static class TestContinuousJfrStarter extends ContinuousJfrStarter {
    private final List<String> _executedCommands = new ArrayList<>();
    private final Set<String> _failingCommands = new HashSet<>();
    private boolean _mBeanAvailable = true;

    @Override
    protected boolean executeDiagnosticCommand(String operationName, String... arguments) {
      String command = operationName;
      if (arguments.length > 0) {
        command += " " + String.join(" ", arguments);
      }
      _executedCommands.add(command);
      return !_failingCommands.contains(operationName);
    }

    @Override
    protected boolean isDiagnosticCommandAvailable() {
      return _mBeanAvailable;
    }

    @Override
    protected boolean isRepositoryCleanupEnabled() {
      return false;
    }

    private void setMBeanAvailable(boolean mBeanAvailable) {
      _mBeanAvailable = mBeanAvailable;
    }

    private void failCommand(String operationName) {
      _failingCommands.add(operationName);
    }

    private List<String> getExecutedCommands() {
      return _executedCommands;
    }
  }
}
