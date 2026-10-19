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
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Exercises driver-side filtering with real local files before dispatching a metadata push.
public class BaseSparkSegmentMetadataPushJobRunnerTest {
  @Test
  public void testPushPatternFiltersBeforeDuplicateValidation()
      throws Exception {
    Path outputDir = Files.createTempDirectory("metadata-push-");
    try {
      Path current = Files.createDirectory(outputDir.resolve("current"));
      Path selected = Files.createFile(current.resolve("segment.tar.zst"));
      Files.createFile(current.resolve("segment.tar.gz"));
      Files.createFile(Files.createDirectory(outputDir.resolve("stale")).resolve("segment.tar.zst"));
      SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
      spec.setOutputDirURI(outputDir.toUri().toString());
      spec.setPinotFSSpecs(List.of());
      PushJobSpec pushSpec = new PushJobSpec();
      pushSpec.setPushParallelism(2);
      pushSpec.setPushFileNamePattern("glob:**/current/*.tar.zst");
      spec.setPushJobSpec(pushSpec);
      List<String> pushed = new ArrayList<>();
      BaseSparkSegmentMetadataPushJobRunner runner = new BaseSparkSegmentMetadataPushJobRunner(spec) {
        @Override
        public void parallelizeMetadataPushJob(List<String> segments, List<PinotFSSpec> fileSystems,
            int parallelism, URI output) {
          pushed.addAll(segments);
        }
      };
      runner.run();
      assertEquals(pushed, List.of(selected.toString()));
      pushSpec.setPushParallelism(0);
      pushSpec.setPushFileNamePattern("glob:**/missing/*");
      runner.run();
      assertEquals(pushed, List.of(selected.toString()));
      pushSpec.setPushFileNamePattern(null);
      assertThrows(IllegalArgumentException.class, runner::run);
    } finally {
      FileUtils.deleteDirectory(outputDir.toFile());
    }
  }
}
