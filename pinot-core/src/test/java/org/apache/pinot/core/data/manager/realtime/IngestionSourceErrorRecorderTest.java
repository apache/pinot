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
package org.apache.pinot.core.data.manager.realtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassifier;
import org.apache.pinot.spi.ingestion.error.RefusedBySourceException;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class IngestionSourceErrorRecorderTest {
  private static final String TABLE = "recreatedTable_REALTIME";

  @BeforeClass
  public void setUp() {
    IngestionSourceErrorClassifier.registerRule(RefusedBySourceException.RULE);
  }

  @AfterClass
  public void tearDown() {
    IngestionSourceErrorClassifier.unregisterRule(RefusedBySourceException.RULE);
  }

  @Test(timeOut = 30_000)
  public void writerThatOutlivesTheShutdownNeverErasesARecreatedTablesRecord()
      throws Exception {
    ServerMetrics metrics = spy(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    String authGauge = ServerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS.getGaugeName() + "." + TABLE + ".0";
    // The old table's consumer thread stalls in the middle of its write
    CountDownLatch writing = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    doAnswer(invocation -> {
      if (writing.getCount() > 0) {
        writing.countDown();
        resume.await();
      }
      return invocation.callRealMethod();
    }).when(metrics).setValueOfPartitionGauge(anyString(), anyInt(), any(ServerGauge.class), anyLong());

    IngestionSourceErrorRecorder oldTable = new IngestionSourceErrorRecorder(metrics, TABLE);
    Thread oldWriter =
        new Thread(() -> oldTable.record("fetch", new RefusedBySourceException(), false, "topic", List.of(0)));
    oldWriter.start();
    assertTrue(writing.await(10, TimeUnit.SECONDS));
    Thread shutdown = new Thread(oldTable::close);
    shutdown.start();

    // The table is deleted and recreated as soon as the old one's shutdown returns
    IngestionSourceErrorRecorder newTable = new IngestionSourceErrorRecorder(metrics, TABLE);
    shutdown.join(1_000);
    boolean newTableRecorded = !shutdown.isAlive();
    if (newTableRecorded) {
      newTable.record("fetch", new RefusedBySourceException(), false, "topic", List.of(0));
    }
    resume.countDown();
    oldWriter.join();
    shutdown.join();
    if (!newTableRecorded) {
      newTable.record("fetch", new RefusedBySourceException(), false, "topic", List.of(0));
    }
    assertNotNull(metrics.getGaugeValue(authGauge), "The recreated table's record must survive the old shutdown");
  }

  /// Every consumer thread of every table records through the same process: a failure in recording itself, on many
  /// threads at once, must never reach the consume loop, and is logged at WARN once per interval for the process.
  @Test(timeOut = 30_000)
  public void concurrentRecordingFailuresNeverEscapeAndWarnOnce()
      throws Exception {
    ServerMetrics metrics = spy(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    doThrow(new IllegalStateException("metrics registry closed")).when(metrics)
        .setValueOfPartitionGauge(anyString(), anyInt(), any(ServerGauge.class), anyLong());
    // As in a fresh process: another test class in this JVM may have logged a recording failure within the interval
    IngestionSourceErrorRecorder.LAST_RECORDING_FAILURE_LOG_MS.set(0);
    WarnCapture warnings = WarnCapture.attachTo(IngestionSourceErrorRecorder.class.getName());
    int numThreads = 8;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      List<Future<?>> writers = new ArrayList<>();
      for (int i = 0; i < numThreads; i++) {
        IngestionSourceErrorRecorder recorder = new IngestionSourceErrorRecorder(metrics, "table" + i + "_REALTIME");
        writers.add(executor.submit(() -> {
          for (int j = 0; j < 1_000; j++) {
            recorder.record("fetch", new RefusedBySourceException(), false, "topic", List.of(0));
          }
        }));
      }
      for (Future<?> writer : writers) {
        writer.get();
      }
    } finally {
      executor.shutdownNow();
      warnings.detach();
    }
    assertEquals(warnings.messagesStartingWith("Failed to record an ingestion source failure").size(), 1);
  }

  /// Captures the WARN lines of one logger
  private static final class WarnCapture extends AbstractAppender {
    private final List<String> _messages = new CopyOnWriteArrayList<>();
    private final Logger _logger;

    private WarnCapture(Logger logger) {
      super("IngestionSourceErrorRecorderTestCapture", null, null, true, Property.EMPTY_ARRAY);
      _logger = logger;
    }

    static WarnCapture attachTo(String loggerName) {
      LoggerContext context = (LoggerContext) LogManager.getContext(false);
      WarnCapture capture = new WarnCapture(context.getLogger(loggerName));
      capture.start();
      capture._logger.addAppender(capture);
      return capture;
    }

    void detach() {
      _logger.removeAppender(this);
      stop();
      // Attaching registered a LoggerConfig for a logger log4j2.xml doesn't declare: remove it, since test classes
      // share this JVM
      LoggerContext context = _logger.getContext();
      context.getConfiguration().removeLogger(_logger.getName());
      context.updateLoggers();
    }

    List<String> messagesStartingWith(String prefix) {
      return _messages.stream().filter(message -> message.startsWith(prefix)).collect(Collectors.toList());
    }

    @Override
    public void append(LogEvent event) {
      if (event.getLevel() == Level.WARN) {
        _messages.add(event.getMessage().getFormattedMessage());
      }
    }
  }
}
