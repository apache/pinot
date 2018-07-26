/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.server.util;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.util.AvroUtils;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class SegmentTestUtils {
  private SegmentTestUtils() {
  }

  @Nonnull
  public static SegmentGeneratorConfig getSegmentGeneratorConfig(@Nonnull File inputAvro, @Nonnull File outputDir,
      @Nonnull TimeUnit timeUnit, @Nonnull String tableName, @Nullable Schema pinotSchema)
      throws IOException {
    SegmentGeneratorConfig segmentGeneratorConfig;
    if (pinotSchema == null) {
      segmentGeneratorConfig = new SegmentGeneratorConfig(AvroUtils.getPinotSchemaFromAvroDataFile(inputAvro));
    } else {
      segmentGeneratorConfig = new SegmentGeneratorConfig(pinotSchema);
    }

    segmentGeneratorConfig.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGeneratorConfig.setSegmentTimeUnit(timeUnit);
    if (inputAvro.getName().endsWith("gz")) {
      segmentGeneratorConfig.setFormat(FileFormat.GZIPPED_AVRO);
    } else {
      segmentGeneratorConfig.setFormat(FileFormat.AVRO);
    }
    segmentGeneratorConfig.setSegmentVersion(SegmentVersion.v1);
    segmentGeneratorConfig.setTableName(tableName);
    segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath());

    return segmentGeneratorConfig;
  }
}
