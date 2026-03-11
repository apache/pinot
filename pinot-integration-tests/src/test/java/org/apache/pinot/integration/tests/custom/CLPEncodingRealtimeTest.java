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
package org.apache.pinot.integration.tests.custom;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(suiteName = "CustomClusterIntegrationTest")
public class CLPEncodingRealtimeTest extends CustomDataQueryClusterIntegrationTest {

  private final FieldConfig.CompressionCodec _selectedCompressionCodec =
      RANDOM.nextBoolean() ? FieldConfig.CompressionCodec.CLP : FieldConfig.CompressionCodec.CLPV2;

  @Override
  public String getTableName() {
    return "CLPEncodingRealtimeTest";
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension("logLine", FieldSpec.DataType.STRING)
        .addDateTimeField("timestampInEpoch", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    return unpackTarData("clpEncodingITData.tar.gz", _tempDir);
  }

  @Override
  public String getTimeColumnName() {
    return "timestampInEpoch";
  }

  @Override
  protected long getCountStarResult() {
    return 100;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return 30;
  }

  @Nullable
  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return Collections.singletonList("logLine");
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    fieldConfigs.add(new FieldConfig.Builder("logLine").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(_selectedCompressionCodec).build());
    return fieldConfigs;
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    List<TransformConfig> transforms = new ArrayList<>();
    transforms.add(new TransformConfig("timestampInEpoch", "now()"));

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transforms);
    return ingestionConfig;
  }

  @Test
  public void testValues()
      throws Exception {
    Assert.assertEquals(getPinotConnection().execute(
            "SELECT count(*) FROM " + getTableName() + " WHERE REGEXP_LIKE(logLine, '.*executor.*')").getResultSet(0)
        .getLong(0), 53);
  }
}
