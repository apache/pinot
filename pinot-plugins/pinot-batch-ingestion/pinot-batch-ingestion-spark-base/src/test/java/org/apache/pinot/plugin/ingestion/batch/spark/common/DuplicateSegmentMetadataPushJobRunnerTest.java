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
package org.apache.pinot.plugin.ingestion.batch.spark.common;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Exercises driver-side filtering with real local files before dispatching segment pushes.
public class DuplicateSegmentMetadataPushJobRunnerTest {
  @DataProvider
  public Object[][] pushModes() {
    return new Object[][]{{"metadata"}, {"tar"}, {"uri"}};
  }

  @Test(dataProvider = "pushModes")
  public void testPushPatternFiltersBeforeDuplicateValidation(String mode)
      throws Exception {
    Path outputDir = Files.createTempDirectory("segment-push-% ");
    try {
      Path current = Files.createDirectory(outputDir.resolve("current"));
      Path selected = Files.createFile(current.resolve("segment.tar.gz"));
      Files.createFile(Files.createDirectory(outputDir.resolve("stale")).resolve("segment.tar.gz"));
      SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
      spec.setOutputDirURI(outputDir.toUri().toString());
      spec.setPinotFSSpecs(List.of());
      PushJobSpec pushSpec = new PushJobSpec();
      pushSpec.setPushParallelism(2);
      pushSpec.setPushFileNamePattern("glob:**/current/*.tar.gz");
      spec.setPushJobSpec(pushSpec);
      List<List<String>> pushed = new ArrayList<>();
      IngestionJobRunner runner = switch (mode) {
        case "metadata" -> new BaseSparkSegmentMetadataPushJobRunner(spec) {
          @Override
          public void parallelizeMetadataPushJob(List<String> segments, List<PinotFSSpec> fileSystems,
              int parallelism, URI output) {
            pushed.add(List.copyOf(segments));
          }
        };
        case "tar" -> new BaseSparkSegmentTarPushJobRunner(spec) {
          @Override
          public void parallelizeTarPushJob(List<PinotFSSpec> fileSystems, List<String> segments,
              int parallelism, URI output) {
            pushed.add(List.copyOf(segments));
          }
        };
        case "uri" -> new BaseSparkSegmentUriPushJobRunner(spec) {
          @Override
          public void parallelizeUriPushJob(List<PinotFSSpec> fileSystems, List<String> segments, int parallelism) {
            pushed.add(List.copyOf(segments));
          }
        };
        default -> throw new IllegalArgumentException(mode);
      };
      List<List<String>> selectedPaths = List.of(
          List.of(mode.equals("uri") ? selected.toFile().toURI().toString() : selected.toString()));
      runner.run();
      assertEquals(pushed, selectedPaths);
      pushSpec.setPushParallelism(0);
      pushSpec.setPushFileNamePattern("glob:**/missing/*");
      runner.run();
      assertEquals(pushed, selectedPaths);
      pushSpec.setPushFileNamePattern(null);
      assertThrows(IllegalArgumentException.class, runner::run);
      assertEquals(pushed, selectedPaths);
    } finally {
      FileUtils.deleteDirectory(outputDir.toFile());
    }
  }
}
