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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SegmentUriPushJobRunnerTest {

  @Test
  public void testPushSegmentsWithoutArchiveExtensionSelectedByPattern()
      throws Exception {
    File outputDir = Files.createTempDirectory("segmentUriPush").toFile();
    try {
      // Segments copied from the deep store (e.g. committed REALTIME segments) have no archive extension
      File subDir = new File(outputDir, "myTable_2026");
      assertTrue(subDir.mkdir());
      File realtimeSegment = new File(outputDir, "myTable__0__0__20260601T1557Z");
      File nestedRealtimeSegment = new File(subDir, "myTable__0__1__20260602T1557Z");
      File tmpSegment = new File(outputDir, "myTable__0__0__20260601T1557Z.tmp." + UUID.randomUUID());
      File offlineSegment = new File(outputDir, "myTable_offline.tar.gz");
      File metadataFile = new File(outputDir, "myTable_offline.metadata.tar.gz");
      File otherFile = new File(outputDir, "_SUCCESS");
      for (File file : List.of(realtimeSegment, nestedRealtimeSegment, tmpSegment, offlineSegment, metadataFile,
          otherFile)) {
        assertTrue(file.createNewFile());
      }

      PushJobSpec pushJobSpec = new PushJobSpec();
      SegmentUriPushJobRunner runner = createRunner(outputDir, pushJobSpec);
      // Without allowSegmentsWithoutArchiveExtension, only segment archives are selected even with a pattern
      assertEquals(getFileNames(runner.getSegmentUriToTarPathMap()), List.of("myTable_offline.tar.gz"));
      pushJobSpec.setPushFileNamePattern("glob:**/myTable*");
      runner = createRunner(outputDir, pushJobSpec);
      assertEquals(getFileNames(runner.getSegmentUriToTarPathMap()), List.of("myTable_offline.tar.gz"));

      // Directories and temporary segment files are skipped even when matched by the pattern
      pushJobSpec.setAllowSegmentsWithoutArchiveExtension(true);
      runner = createRunner(outputDir, pushJobSpec);
      Map<String, String> segmentUriToTarPathMap = runner.getSegmentUriToTarPathMap();
      assertEquals(getFileNames(segmentUriToTarPathMap),
          List.of("myTable__0__0__20260601T1557Z", "myTable__0__1__20260602T1557Z", "myTable_offline.tar.gz"));
      assertEquals(runner.getSegmentsToReplace(segmentUriToTarPathMap).stream().sorted().toList(),
          List.of("myTable__0__0__20260601T1557Z", "myTable__0__1__20260602T1557Z", "myTable_offline"));
    } finally {
      FileUtils.deleteQuietly(outputDir);
    }
  }

  private static List<String> getFileNames(Map<String, String> segmentUriToTarPathMap) {
    return segmentUriToTarPathMap.values().stream().map(path -> new File(path).getName()).sorted().toList();
  }

  private static SegmentUriPushJobRunner createRunner(File outputDir, PushJobSpec pushJobSpec) {
    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    spec.setOutputDirURI(outputDir.toURI().toString());
    spec.setPushJobSpec(pushJobSpec);
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName("myTable");
    spec.setTableSpec(tableSpec);
    PinotFSSpec pinotFSSpec = new PinotFSSpec();
    pinotFSSpec.setScheme("file");
    pinotFSSpec.setClassName(LocalPinotFS.class.getName());
    spec.setPinotFSSpecs(List.of(pinotFSSpec));
    SegmentUriPushJobRunner runner = new SegmentUriPushJobRunner(spec);
    runner.initFileSys();
    return runner;
  }
}
