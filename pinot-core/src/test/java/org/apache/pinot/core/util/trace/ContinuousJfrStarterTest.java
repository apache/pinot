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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import jdk.jfr.RecordingState;
import jdk.jfr.consumer.RecordingFile;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


/// Tests [ContinuousJfrStarter] against the real JFR of the test JVM.
public class ContinuousJfrStarterTest {
  private static final String SERVER_ID = "Server_pinot-server-0.pinot-server-headless_8098";
  private static final String BROKER_ID = "Broker_pinot-broker-0.pinot-broker-headless_8099";
  private static final Set<String> ENABLED_CHANGED = Set.of("pinot.jfr.enabled");
  private static final Map<String, String> ENABLED = Map.of("pinot.jfr.enabled", "true");
  private static final Map<String, String> DISABLED = Map.of("pinot.jfr.enabled", "false");

  private final List<ContinuousJfrStarter> _starters = new ArrayList<>();
  private ContinuousJfrStarter _server;

  @BeforeMethod
  public void setUp() {
    _server = starter(SERVER_ID, List.of(), false);
  }

  /// Closes the recordings and deletes the dumps of all the instances that the test created.
  @AfterMethod
  public void tearDown()
      throws IOException {
    for (ContinuousJfrStarter starter : _starters) {
      Recording recording = starter.getRecording();
      starter.close();
      if (recording != null) {
        recording.close();
      }
      Path dump = starter.getLastDump();
      if (dump != null) {
        Files.deleteIfExists(dump);
      }
    }
    _starters.clear();
  }

  private ContinuousJfrStarter starter(String instanceId, List<String> jvmArguments, boolean jvmShuttingDown) {
    ContinuousJfrStarter starter = new ContinuousJfrStarter(instanceId, jvmArguments, () -> jvmShuttingDown);
    _starters.add(starter);
    return starter;
  }

  /// Asserts that the dump is a readable JFR file in the repository directory of the JVM.
  private static void assertIsADumpInTheRepository(Path dump)
      throws IOException {
    assertThat(dump).isNotNull().exists();
    assertThat(dump.getParent()).isEqualTo(Path.of(System.getProperty("jdk.jfr.repository")));
    assertThat(dump.getFileName().toString()).startsWith(ContinuousJfrStarter.DEFAULT_NAME + "-").endsWith(".jfr");
    RecordingFile.readAllEvents(dump);
  }

  @Test
  public void disabledByDefault() {
    _server.onChange(Set.of(), Map.of());

    assertThat(_server.isRunning()).isFalse();
    assertThat(_server.getRecording()).isNull();
  }

  @Test
  public void startsRecordingWithDefaults()
      throws Exception {
    _server.onChange(Set.of(), ENABLED);

    assertThat(_server.isRunning()).isTrue();
    Recording recording = _server.getRecording();
    assertThat(recording.getState()).isEqualTo(RecordingState.RUNNING);
    assertThat(recording.getName()).isEqualTo(ContinuousJfrStarter.DEFAULT_NAME);
    assertThat(recording.isToDisk()).isTrue();
    assertThat(recording.getDumpOnExit()).isFalse();
    assertThat(recording.getMaxSize()).isEqualTo(DataSizeUtils.toBytes(ContinuousJfrStarter.DEFAULT_MAX_SIZE));
    assertThat(recording.getMaxAge()).isEqualTo(Duration.ofDays(7));
    assertThat(recording.getSettings()).isEqualTo(Configuration.getConfiguration("default").getSettings());
  }

  @Test
  public void disableDumpsTheRecordingAndClosesIt()
      throws IOException {
    _server.onChange(Set.of(), ENABLED);
    Recording recording = _server.getRecording();

    _server.onChange(ENABLED_CHANGED, DISABLED);

    assertThat(_server.isRunning()).isFalse();
    // Recording.close() deletes the chunks of the recording, so the recorded data survives only in the dump.
    assertThat(recording.getState()).isEqualTo(RecordingState.CLOSED);
    assertIsADumpInTheRepository(_server.getLastDump());
  }

  @Test
  public void keepsOnlyTheLastDump()
      throws Exception {
    _server.onChange(Set.of(), ENABLED);
    _server.onChange(ENABLED_CHANGED, DISABLED);
    Path firstDump = _server.getLastDump();
    // The file name has a timestamp in milliseconds.
    Thread.sleep(5);
    _server.onChange(ENABLED_CHANGED, ENABLED);
    _server.onChange(ENABLED_CHANGED, DISABLED);
    Path secondDump = _server.getLastDump();

    assertThat(secondDump).isNotEqualTo(firstDump);
    assertThat(firstDump).doesNotExist();
    assertIsADumpInTheRepository(secondDump);
  }

  @Test
  public void ignoresChangesToOtherConfigs() {
    _server.onChange(Set.of(), DISABLED);

    _server.onChange(Set.of("some.other.config"), Map.of("pinot.jfr.enabled", "true", "some.other.config", "true"));

    assertThat(_server.isRunning()).isFalse();
  }

  @Test
  public void retunesTheRunningRecordingInPlace()
      throws Exception {
    _server.onChange(Set.of(), ENABLED);
    Recording recording = _server.getRecording();

    _server.onChange(Set.of("pinot.jfr.configuration", "pinot.jfr.maxSize", "pinot.jfr.maxAge", "pinot.jfr.name"),
        Map.of("pinot.jfr.enabled", "true", "pinot.jfr.configuration", "profile", "pinot.jfr.maxSize", "10MB",
            "pinot.jfr.maxAge", "PT1H", "pinot.jfr.name", "renamed"));

    assertThat(_server.getRecording()).isSameAs(recording);
    assertThat(recording.getState()).isEqualTo(RecordingState.RUNNING);
    assertThat(recording.getName()).isEqualTo("renamed");
    assertThat(recording.getSettings()).isEqualTo(Configuration.getConfiguration("profile").getSettings());
    assertThat(recording.getMaxSize()).isEqualTo(DataSizeUtils.toBytes("10MB"));
    assertThat(recording.getMaxAge()).isEqualTo(Duration.ofHours(1));
  }

  @Test
  public void restartsTheRecordingWhenToDiskChanges()
      throws IOException {
    _server.onChange(Set.of(), ENABLED);
    Recording diskRecording = _server.getRecording();

    _server.onChange(Set.of("pinot.jfr.toDisk"), Map.of("pinot.jfr.enabled", "true", "pinot.jfr.toDisk", "false"));

    Recording memoryRecording = _server.getRecording();
    assertThat(memoryRecording).isNotSameAs(diskRecording);
    assertThat(memoryRecording.getState()).isEqualTo(RecordingState.RUNNING);
    assertThat(memoryRecording.isToDisk()).isFalse();
    assertThat(diskRecording.getState()).isEqualTo(RecordingState.CLOSED);
    assertIsADumpInTheRepository(_server.getLastDump());
  }

  @Test
  public void acceptsAJfcFilePath() {
    Path profile = Path.of(System.getProperty("java.home"), "lib", "jfr", "profile.jfc");

    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true", "pinot.jfr.configuration", profile.toString()));

    assertThat(_server.isRunning()).isTrue();
  }

  @Test
  public void recordsWhenTheRegexMatchesTheFullInstanceId() {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true", "pinot.jfr.instanceRegex", "Server_.*"));

    assertThat(_server.isRunning()).isTrue();
  }

  @Test
  public void doesNotRecordWhenTheRegexMatchesOnlyPartOfTheInstanceId() {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true", "pinot.jfr.instanceRegex", "Server"));

    assertThat(_server.isRunning()).isFalse();
  }

  @Test
  public void regexChangeStopsTheRecordingLive() {
    _server.onChange(Set.of(), ENABLED);
    assertThat(_server.isRunning()).isTrue();

    _server.onChange(Set.of("pinot.jfr.instanceRegex"),
        Map.of("pinot.jfr.enabled", "true", "pinot.jfr.instanceRegex", "Broker_.*"));

    assertThat(_server.isRunning()).isFalse();
  }

  @Test
  public void regexSelectsOneRoleAmongComponentsInTheSameJvm() {
    ContinuousJfrStarter broker = starter(BROKER_ID, List.of(), false);
    Map<String, String> config = Map.of("pinot.jfr.enabled", "true", "pinot.jfr.instanceRegex", "Broker_.*");

    _server.onChange(Set.of(), config);
    broker.onChange(Set.of(), config);

    assertThat(_server.isRunning()).isFalse();
    assertThat(broker.isRunning()).isTrue();
  }

  @Test
  public void invalidRegexKeepsTheCurrentState() {
    _server.onChange(Set.of(), ENABLED);
    Recording recording = _server.getRecording();

    _server.onChange(Set.of("pinot.jfr.instanceRegex"),
        Map.of("pinot.jfr.enabled", "true", "pinot.jfr.instanceRegex", "Server_("));

    assertThat(_server.isRunning()).isTrue();
    assertThat(recording.getState()).isEqualTo(RecordingState.RUNNING);
  }

  @Test
  public void invalidRecordingOptionsKeepTheCurrentState() {
    _server.onChange(Set.of(), ENABLED);
    Recording recording = _server.getRecording();

    _server.onChange(Set.of("pinot.jfr.maxSize"), Map.of("pinot.jfr.enabled", "true", "pinot.jfr.maxSize", "lots"));
    _server.onChange(Set.of("pinot.jfr.configuration"),
        Map.of("pinot.jfr.enabled", "true", "pinot.jfr.configuration", "no-such-configuration"));

    assertThat(_server.getRecording()).isSameAs(recording);
    assertThat(recording.getState()).isEqualTo(RecordingState.RUNNING);
    assertThat(recording.getMaxSize()).isEqualTo(DataSizeUtils.toBytes(ContinuousJfrStarter.DEFAULT_MAX_SIZE));
  }

  @Test
  public void doesNothingWhenTheJvmStartsARecording() {
    ContinuousJfrStarter starter =
        starter(SERVER_ID, List.of("-Xmx4g", "-XX:StartFlightRecording=name=jvm,settings=default"), false);

    starter.onChange(Set.of(), ENABLED);

    assertThat(starter.isRunning()).isFalse();
  }

  @Test
  public void recordsWhenTheJvmOnlySetsFlightRecorderOptions() {
    ContinuousJfrStarter starter =
        starter(SERVER_ID, List.of("-XX:FlightRecorderOptions:repository=/var/pinot/jfr,preserve-repository=true"),
            false);

    starter.onChange(Set.of(), ENABLED);

    assertThat(starter.isRunning()).isTrue();
  }

  @Test
  public void ignoresTheRemovedRepositoryConfigs() {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true", "pinot.jfr.directory", "/does/not/exist",
        "pinot.jfr.preserveRepository", "true", "pinot.jfr.repositoryMaxTotalSize", "1GB",
        "pinot.jfr.maxDumps", "3"));

    assertThat(_server.isRunning()).isTrue();
  }

  @Test
  public void componentsInTheSameJvmRecordIndependently() {
    ContinuousJfrStarter broker = starter(BROKER_ID, List.of(), false);
    _server.onChange(Set.of(), ENABLED);
    broker.onChange(Set.of(), ENABLED);
    Recording serverRecording = _server.getRecording();
    Recording brokerRecording = broker.getRecording();
    assertThat(brokerRecording).isNotSameAs(serverRecording);

    _server.onChange(ENABLED_CHANGED, DISABLED);

    assertThat(serverRecording.getState()).isEqualTo(RecordingState.CLOSED);
    assertThat(brokerRecording.getState()).isEqualTo(RecordingState.RUNNING);
  }

  @Test
  public void closeDumpsAndClosesTheRecording()
      throws IOException {
    // The public constructor reads the real JVM arguments and the real shutdown state, and the JVM is not shutting
    // down.
    ContinuousJfrStarter starter = new ContinuousJfrStarter(SERVER_ID);
    _starters.add(starter);
    starter.onChange(Set.of(), ENABLED);
    Recording recording = starter.getRecording();

    starter.close();

    assertThat(starter.isRunning()).isFalse();
    assertThat(recording.getState()).isEqualTo(RecordingState.CLOSED);
    assertIsADumpInTheRepository(starter.getLastDump());
  }

  @Test
  public void closeLeavesTheRecordingRunningWhileTheJvmShutsDown() {
    ContinuousJfrStarter starter = starter(SERVER_ID, List.of(), true);
    starter.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true", "pinot.jfr.dumpOnExit", "true"));
    Recording recording = starter.getRecording();

    starter.close();

    // The shutdown hook of JFR dumps only the recordings that still run.
    assertThat(recording.getState()).isEqualTo(RecordingState.RUNNING);
    assertThat(starter.isRunning()).isFalse();
    assertThat(starter.getRecording()).isNull();
    recording.close();
  }

  @Test
  public void closedInstanceIgnoresLaterConfigChanges() {
    _server.onChange(Set.of(), ENABLED);
    _server.close();

    _server.onChange(ENABLED_CHANGED, Map.of("pinot.jfr.enabled", "true", "pinot.jfr.maxSize", "1GB"));

    assertThat(_server.isRunning()).isFalse();
    assertThat(_server.getRecording()).isNull();
  }

  @Test
  public void restartedComponentStartsANewRecording() {
    _server.onChange(Set.of(), ENABLED);
    Recording recording = _server.getRecording();
    _server.close();

    // A component that restarts in the same JVM creates a new instance with the same instance ID.
    ContinuousJfrStarter restarted = starter(SERVER_ID, List.of(), false);
    restarted.onChange(Set.of(), ENABLED);

    assertThat(recording.getState()).isEqualTo(RecordingState.CLOSED);
    assertThat(restarted.isRunning()).isTrue();
    assertThat(restarted.getRecording()).isNotSameAs(recording);
  }

  @Test
  public void replacesARecordingClosedOutsidePinot() {
    Map<String, String> config = new HashMap<>(ENABLED);
    _server.onChange(Set.of(), config);
    Recording closedRecording = _server.getRecording();

    // jcmd <pid> JFR.stop closes the recording.
    closedRecording.close();
    assertThat(_server.isRunning()).isFalse();

    config.put("pinot.jfr.maxSize", "1GB");
    _server.onChange(Set.of("pinot.jfr.maxSize"), config);

    assertThat(_server.isRunning()).isTrue();
    Recording newRecording = _server.getRecording();
    assertThat(newRecording).isNotSameAs(closedRecording);
    assertThat(newRecording.getState()).isEqualTo(RecordingState.RUNNING);
    assertThat(newRecording.getMaxSize()).isEqualTo(DataSizeUtils.toBytes("1GB"));
  }
}
