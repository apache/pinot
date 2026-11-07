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

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import jdk.jfr.RecordingState;
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

  private ContinuousJfrStarter _server;

  @BeforeMethod
  public void setUp() {
    ContinuousJfrStarter.resetGlobalStateForTesting();
    _server = new ContinuousJfrStarter(SERVER_ID, List.of());
  }

  @AfterMethod
  public void tearDown() {
    ContinuousJfrStarter.resetGlobalStateForTesting();
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
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));

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
  public void stopKeepsTheRecordedData() {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));
    Recording recording = _server.getRecording();

    _server.onChange(ENABLED_CHANGED, Map.of("pinot.jfr.enabled", "false"));

    assertThat(_server.isRunning()).isFalse();
    // A closed recording deletes its chunks from the repository. A stopped one keeps them.
    assertThat(recording.getState()).isEqualTo(RecordingState.STOPPED);
    assertThat(ContinuousJfrStarter.getStoppedRecording(ContinuousJfrStarter.DEFAULT_NAME)).isSameAs(recording);
  }

  @Test
  public void keepsOnlyTheLastStoppedRecording() {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));
    Recording first = _server.getRecording();
    _server.onChange(ENABLED_CHANGED, Map.of("pinot.jfr.enabled", "false"));
    _server.onChange(ENABLED_CHANGED, Map.of("pinot.jfr.enabled", "true"));
    Recording second = _server.getRecording();
    _server.onChange(ENABLED_CHANGED, Map.of("pinot.jfr.enabled", "false"));

    assertThat(first.getState()).isEqualTo(RecordingState.CLOSED);
    assertThat(second.getState()).isEqualTo(RecordingState.STOPPED);
  }

  @Test
  public void ignoresChangesToOtherConfigs() {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "false"));

    _server.onChange(Set.of("some.other.config"), Map.of("pinot.jfr.enabled", "true", "some.other.config", "true"));

    assertThat(_server.isRunning()).isFalse();
  }

  @Test
  public void retunesTheRunningRecordingInPlace()
      throws Exception {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));
    Recording recording = _server.getRecording();

    _server.onChange(Set.of("pinot.jfr.configuration", "pinot.jfr.maxSize", "pinot.jfr.maxAge"),
        Map.of("pinot.jfr.enabled", "true", "pinot.jfr.configuration", "profile", "pinot.jfr.maxSize", "10MB",
            "pinot.jfr.maxAge", "PT1H"));

    assertThat(_server.getRecording()).isSameAs(recording);
    assertThat(recording.getState()).isEqualTo(RecordingState.RUNNING);
    assertThat(recording.getSettings()).isEqualTo(Configuration.getConfiguration("profile").getSettings());
    assertThat(recording.getMaxSize()).isEqualTo(DataSizeUtils.toBytes("10MB"));
    assertThat(recording.getMaxAge()).isEqualTo(Duration.ofHours(1));
  }

  @Test
  public void restartsTheRecordingWhenToDiskChanges() {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));
    Recording diskRecording = _server.getRecording();

    _server.onChange(Set.of("pinot.jfr.toDisk"), Map.of("pinot.jfr.enabled", "true", "pinot.jfr.toDisk", "false"));

    Recording memoryRecording = _server.getRecording();
    assertThat(memoryRecording).isNotSameAs(diskRecording);
    assertThat(memoryRecording.getState()).isEqualTo(RecordingState.RUNNING);
    assertThat(memoryRecording.isToDisk()).isFalse();
    assertThat(diskRecording.getState()).isEqualTo(RecordingState.STOPPED);
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
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));
    assertThat(_server.isRunning()).isTrue();

    _server.onChange(Set.of("pinot.jfr.instanceRegex"),
        Map.of("pinot.jfr.enabled", "true", "pinot.jfr.instanceRegex", "Broker_.*"));

    assertThat(_server.isRunning()).isFalse();
  }

  @Test
  public void regexSelectsOneRoleAmongComponentsInTheSameJvm() {
    ContinuousJfrStarter broker = new ContinuousJfrStarter(BROKER_ID, List.of());
    Map<String, String> config = Map.of("pinot.jfr.enabled", "true", "pinot.jfr.instanceRegex", "Broker_.*");

    _server.onChange(Set.of(), config);
    broker.onChange(Set.of(), config);

    assertThat(_server.isRunning()).isFalse();
    assertThat(broker.isRunning()).isTrue();
  }

  @Test
  public void invalidRegexKeepsTheCurrentState() {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));
    Recording recording = _server.getRecording();

    _server.onChange(Set.of("pinot.jfr.instanceRegex"),
        Map.of("pinot.jfr.enabled", "true", "pinot.jfr.instanceRegex", "Server_("));

    assertThat(_server.isRunning()).isTrue();
    assertThat(recording.getState()).isEqualTo(RecordingState.RUNNING);
  }

  @Test
  public void invalidRecordingOptionsKeepTheCurrentState() {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));
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
    ContinuousJfrStarter starter = new ContinuousJfrStarter(SERVER_ID,
        List.of("-Xmx4g", "-XX:StartFlightRecording=name=jvm,settings=default"));

    starter.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));

    assertThat(starter.isRunning()).isFalse();
  }

  @Test
  public void recordsWhenTheJvmOnlySetsFlightRecorderOptions() {
    ContinuousJfrStarter starter = new ContinuousJfrStarter(SERVER_ID,
        List.of("-XX:FlightRecorderOptions:repository=/var/pinot/jfr,preserve-repository=true"));

    starter.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true"));

    assertThat(starter.isRunning()).isTrue();
  }

  @Test
  public void ignoresTheRemovedRepositoryConfigs() {
    _server.onChange(Set.of(), Map.of("pinot.jfr.enabled", "true", "pinot.jfr.directory", "/does/not/exist",
        "pinot.jfr.preserveRepository", "true", "pinot.jfr.repositoryMaxTotalSize", "1GB"));

    assertThat(_server.isRunning()).isTrue();
  }

  @Test
  public void componentsInTheSameJvmShareOneRecording() {
    ContinuousJfrStarter broker = new ContinuousJfrStarter(BROKER_ID, List.of());
    Map<String, String> enabled = Map.of("pinot.jfr.enabled", "true");
    Map<String, String> disabled = Map.of("pinot.jfr.enabled", "false");

    _server.onChange(Set.of(), enabled);
    broker.onChange(Set.of(), enabled);
    assertThat(broker.getRecording()).isSameAs(_server.getRecording());
    Recording recording = _server.getRecording();

    _server.onChange(ENABLED_CHANGED, disabled);
    assertThat(recording.getState()).isEqualTo(RecordingState.RUNNING);

    broker.onChange(ENABLED_CHANGED, disabled);
    assertThat(recording.getState()).isEqualTo(RecordingState.STOPPED);
  }

  @Test
  public void renameMovesTheInstanceToANewRecording() {
    Map<String, String> config = new HashMap<>(Map.of("pinot.jfr.enabled", "true"));
    _server.onChange(Set.of(), config);
    Recording oldRecording = _server.getRecording();

    config.put("pinot.jfr.name", "renamed");
    _server.onChange(Set.of("pinot.jfr.name"), config);

    assertThat(_server.getRecording().getName()).isEqualTo("renamed");
    assertThat(oldRecording.getState()).isEqualTo(RecordingState.STOPPED);
  }
}
