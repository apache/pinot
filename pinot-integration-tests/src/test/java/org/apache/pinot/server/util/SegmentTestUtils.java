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
package org.apache.pinot.server.util;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


public class SegmentTestUtils {
  private SegmentTestUtils() {
  }

  @Nonnull
  public static SegmentGeneratorConfig getSegmentGeneratorConfig(@Nonnull File inputAvro, @Nonnull File outputDir,
      @Nonnull String timeColumnName, @Nonnull TimeUnit timeUnit, @Nonnull String tableName, @Nullable Schema pinotSchema)
      throws IOException {
    if (pinotSchema == null) {
      pinotSchema = AvroUtils.getPinotSchemaFromAvroDataFile(inputAvro);
    }
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTimeColumnName(timeColumnName).setTableName(tableName).build();
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, pinotSchema);

    segmentGeneratorConfig.setInputFilePath(inputAvro.getAbsolutePath());
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
