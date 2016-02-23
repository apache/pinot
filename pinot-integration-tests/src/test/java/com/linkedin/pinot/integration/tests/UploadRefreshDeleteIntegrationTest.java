/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.util.TestUtils;


/**
 * Test that uploads, refreshes and deletes segments from multiple threads and checks that the row count in Pinot
 * matches with the expected count.
 *
 * @author jfim
 */
public class UploadRefreshDeleteIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(UploadRefreshDeleteIntegrationTest.class);
  private final File _tmpDir = new File("/tmp/" + getClass().getSimpleName());
  private final File _segmentsDir = new File(_tmpDir, "segments");
  private final File _tarsDir = new File(_tmpDir, "tars");

  @Override
  public void testMultipleQueries() throws Exception {
    // Ignore this inherited test
  }

  @Override
  public void testHardcodedQuerySet() throws Exception {
    // Ignore this inherited test
  }

  @Override
  public void testGeneratedQueries() throws Exception {
    // Ignore this inherited test
  }

  @Override
  public void testGeneratedQueriesWithMultivalues() throws Exception {
    // Ignore this inherited test
  }

  @BeforeClass
  public void setUp() throws Exception {
    // Start an empty Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    addOfflineTable("mytable", "DaysSinceEpoch", "daysSinceEpoch", 3000, "DAYS", null, null);

    ensureDirectoryExistsAndIsEmpty(_tmpDir);
    ensureDirectoryExistsAndIsEmpty(_segmentsDir);
    ensureDirectoryExistsAndIsEmpty(_tarsDir);
  }

  private void generateAndUploadRandomSegment(String segmentName, int rowCount) throws Exception {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    Schema schema = new Schema.Parser().parse(
        new File(TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource("dummy.avsc"))));
    GenericRecord record = new GenericData.Record(schema);
    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    File avroFile = new File(_tmpDir, segmentName + ".avro");
    fileWriter.create(schema, avroFile);

    for (int i = 0; i < rowCount; i++) {
      record.put(0, random.nextInt());
      fileWriter.append(record);
    }

    fileWriter.close();

    int segmentIndex = Integer.parseInt(segmentName.split("_")[1]);

    File segmentTarDir = new File(_tarsDir, segmentName);
    ensureDirectoryExistsAndIsEmpty(segmentTarDir);
    ExecutorService executor = MoreExecutors.sameThreadExecutor();
    buildSegmentsFromAvro(Collections.singletonList(avroFile), executor, segmentIndex,
        new File(_segmentsDir, segmentName), segmentTarDir, "mytable", false);
    executor.shutdown();
    executor.awaitTermination(1L, TimeUnit.MINUTES);

    for (String segmentFileName : segmentTarDir.list()) {
      File file = new File(segmentTarDir, segmentFileName);
      FileUploadUtils.sendFile("localhost", "8998", "segments", segmentFileName, new FileInputStream(file), file.length(),
          FileUploadUtils.SendFileMethod.POST);
    }

    avroFile.delete();
    FileUtils.deleteQuietly(segmentTarDir);
  }

  @Test
  public void testRefresh() throws Exception {
    final int nAtttempts = 5;
    final String segment6 = "segmentToBeRefreshed_6";
    final int nRows1 = 69;
    generateAndUploadRandomSegment(segment6, nRows1);
    verifyNRows(0, nRows1);
    final int nRows2 = 198;
    LOGGER.info("Segment {} loaded with {} rows, refreshing with {}", segment6, nRows1, nRows2);
    generateAndUploadRandomSegment(segment6, nRows2);
    verifyNRows(nRows1, nRows2);
    // Load another segment while keeping this one in place.
    final String segment9 = "newSegment_9";
    final int nRows3 = 102;
    generateAndUploadRandomSegment(segment9, nRows3);
    verifyNRows(nRows2, nRows2+nRows3);
  }

  // Verify that the number of rows is either the initial value or the final value but not something else.
  private void verifyNRows(int currentNrows, int finalNrows) throws Exception {
    int attempt = 0; long sleepTime = 100;
    long nRows = -1;
    while (attempt < 10) {
      Thread.sleep(sleepTime);
      nRows = getCurrentServingNumDocs();
      //nRows can either be the current value or the final value, not any other.
      if (nRows == currentNrows) {
        sleepTime *= 2;
        attempt++;
      } else  if (nRows == finalNrows) {
        return;
      } else {
        Assert.fail("Found unexpected number of rows " + nRows);
      }
    }
    Assert.fail("Failed to get from " + currentNrows + " to " + finalNrows);
  }

  @Test(enabled = false)
  public void testUploadRefreshDelete() throws Exception {
    final int THREAD_COUNT = 1;
    final int SEGMENT_COUNT = 5;

    final int MIN_ROWS_PER_SEGMENT = 500;
    final int MAX_ROWS_PER_SEGMENT = 1000;

    final int OPERATIONS_PER_ITERATION = 10;
    final int ITERATION_COUNT = 5;

    final double UPLOAD_PROBABILITY = 0.8d;

    final String[] segmentNames = new String[SEGMENT_COUNT];
    final int[] segmentRowCounts = new int[SEGMENT_COUNT];

    for (int i = 0; i < SEGMENT_COUNT; i++) {
      segmentNames[i] = "segment_" + i;
      segmentRowCounts[i] = 0;
    }

    for (int i = 0; i < ITERATION_COUNT; i++) {
      // Create THREAD_COUNT threads
      ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

      // Submit OPERATIONS_PER_ITERATION uploads/deletes
      for (int j = 0; j < OPERATIONS_PER_ITERATION; j++) {
        executorService.submit(new Runnable() {
          @Override
          public void run() {
            try {
              ThreadLocalRandom random = ThreadLocalRandom.current();

              // Pick a random segment
              int segmentIndex = random.nextInt(SEGMENT_COUNT);
              String segmentName = segmentNames[segmentIndex];

              // Pick a random operation
              if (random.nextDouble() < UPLOAD_PROBABILITY) {
                // Upload this segment
                LOGGER.info("Will upload segment {}", segmentName);

                synchronized (segmentName) {
                  // Create a segment with a random number of rows
                  int segmentRowCount = random.nextInt(MIN_ROWS_PER_SEGMENT, MAX_ROWS_PER_SEGMENT);
                  LOGGER.info("Generating and uploading segment {} with {} rows", segmentName, segmentRowCount);
                  generateAndUploadRandomSegment(segmentName, segmentRowCount);

                  // Store the number of rows
                  LOGGER.info("Uploaded segment {} with {} rows", segmentName, segmentRowCount);
                  segmentRowCounts[segmentIndex] = segmentRowCount;
                }
              } else {
                // Delete this segment
                LOGGER.info("Will delete segment {}", segmentName);

                synchronized (segmentName) {
                  // Delete this segment
                  LOGGER.info("Deleting segment {}", segmentName);
                  String reply = sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).
                      forSegmentDelete("myresource", segmentName));
                  LOGGER.info("Deletion returned {}", reply);

                  // Set the number of rows to zero
                  LOGGER.info("Deleted segment {}", segmentName);
                  segmentRowCounts[segmentIndex] = 0;
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        });
      }

      // Await for all tasks to complete
      executorService.shutdown();
      executorService.awaitTermination(5L, TimeUnit.MINUTES);

      // Count number of expected rows
      int expectedRowCount = 0;
      for (int segmentRowCount : segmentRowCounts) {
        expectedRowCount += segmentRowCount;
      }

      // Wait for up to one minute for the row count to match the expected row count
      LOGGER.info("Awaiting for the row count to match {}", expectedRowCount);
      int pinotRowCount = (int) getCurrentServingNumDocs();
      long timeInOneMinute = System.currentTimeMillis() + 60 * 1000L;
      while (System.currentTimeMillis() < timeInOneMinute && pinotRowCount != expectedRowCount) {
        LOGGER.info("Row count is {}, expected {}, awaiting for row count to match", pinotRowCount, expectedRowCount);
        Thread.sleep(5000L);

        try {
          pinotRowCount = (int) getCurrentServingNumDocs();
        } catch (Exception e) {
          LOGGER.warn("Caught exception while sending query to Pinot, retrying", e);
        }
      }

      // Compare row counts
      Assert.assertEquals(pinotRowCount, expectedRowCount, "Expected and actual row counts don't match after waiting one minute");
    }
  }

  @AfterClass
  public void tearDown() {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteQuietly(_tmpDir);
  }

  @Override
  protected int getGeneratedQueryCount() {
    return 0;
  }
}
