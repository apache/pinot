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
package org.apache.pinot.plugin.ingestion.batch.standalone;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SegmentUriPushJobRunnerTest {

  @Test
  public void testPushSegmentsWithoutArchiveExtension()
      throws Exception {
    File outputDir = Files.createTempDirectory("segmentUriPush").toFile();
    try {
      // Segments in the deep store (e.g. committed REALTIME segments) have no archive extension
      File subDir = new File(outputDir, "myTable_2026");
      assertTrue(subDir.mkdir());
      for (File file : List.of(new File(outputDir, "myTable__0__0__20260601T1557Z"),
          new File(subDir, "myTable__0__1__20260602T1557Z"), new File(outputDir, "myTable_offline.tar.gz"),
          new File(outputDir, "myTable_offline.metadata.tar.gz"),
          new File(outputDir, "myTable__0__0__20260601T1557Z.tmp." + UUID.randomUUID()),
          new File(outputDir, "_SUCCESS"))) {
        assertTrue(file.createNewFile());
      }

      AtomicReference<Map<String, String>> uploaded = new AtomicReference<>();
      SegmentUriPushJobRunner runner = new SegmentUriPushJobRunner(createSpec(outputDir)) {
        @Override
        public void uploadSegments(Map<String, String> segmentsUriToTarPathMap) {
          uploaded.set(segmentsUriToTarPathMap);
        }
      };
      runner.run();
      // The sub directory is skipped although it has no dot in its name
      assertEquals(uploaded.get().values().stream().map(path -> new File(path).getName()).sorted().toList(),
          List.of("myTable__0__0__20260601T1557Z", "myTable__0__1__20260602T1557Z", "myTable_offline.tar.gz"));
      assertEquals(runner.getSegmentsToReplace(uploaded.get()).stream().sorted().toList(),
          List.of("myTable__0__0__20260601T1557Z", "myTable__0__1__20260602T1557Z", "myTable_offline"));
    } finally {
      FileUtils.deleteQuietly(outputDir);
    }
  }

  @Test
  public void testCleanUpOutputDirRejectedForSegmentWithoutArchiveExtension()
      throws Exception {
    File outputDir = Files.createTempDirectory("segmentUriPush").toFile();
    try {
      File segment = new File(outputDir, "myTable__0__0__20260601T1557Z");
      assertTrue(segment.createNewFile());
      SegmentGenerationJobSpec spec = createSpec(outputDir);
      spec.setCleanUpOutputDir(true);
      expectThrows(IllegalArgumentException.class, () -> new SegmentUriPushJobRunner(spec).run());
      assertTrue(segment.exists());
    } finally {
      FileUtils.deleteQuietly(outputDir);
    }
  }

  private static SegmentGenerationJobSpec createSpec(File outputDir) {
    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    spec.setOutputDirURI(outputDir.toURI().toString());
    spec.setPushJobSpec(new PushJobSpec());
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName("myTable");
    spec.setTableSpec(tableSpec);
    PinotFSSpec pinotFSSpec = new PinotFSSpec();
    pinotFSSpec.setScheme("file");
    pinotFSSpec.setClassName(LocalPinotFS.class.getName());
    spec.setPinotFSSpecs(List.of(pinotFSSpec));
    return spec;
  }
}
