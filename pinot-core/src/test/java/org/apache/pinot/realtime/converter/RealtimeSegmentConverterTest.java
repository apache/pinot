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
package org.apache.pinot.realtime.converter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RealtimeSegmentConverterTest {

  private static final String STRING_COLUMN1 = "string_col1";
  private static final String STRING_COLUMN2 = "string_col2";
  private static final String STRING_COLUMN3 = "string_col3";
  private static final String STRING_COLUMN4 = "string_col4";
  private static final String LONG_COLUMN1 = "long_col1";
  private static final String LONG_COLUMN2 = "long_col2";
  private static final String LONG_COLUMN3 = "long_col3";
  private static final String LONG_COLUMN4 = "long_col4";
  private static final String MV_INT_COLUMN = "mv_col";
  private static final String DATE_TIME_COLUMN = "date_time_col";

  private static final File TMP_DIR =
      new File(FileUtils.getTempDirectory(), RealtimeSegmentConverterTest.class.getName());

  public void setup() {
    Preconditions.checkState(TMP_DIR.mkdirs());
  }

  @Test
  public void testNoVirtualColumnsInSchema() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("col1", FieldSpec.DataType.STRING)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "col1"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "col2")).build();
    String segmentName = "segment1";
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, segmentName);
    Assert.assertEquals(schema.getColumnNames().size(), 5);
    Schema newSchema = RealtimeSegmentConverter.getUpdatedSchema(schema);
    Assert.assertEquals(newSchema.getColumnNames().size(), 2);
  }

  @Test
  public void testNoRecordsIndexed()
      throws Exception {

    File tmpDir = new File(TMP_DIR, "tmp_" + System.currentTimeMillis());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName(DATE_TIME_COLUMN)
            .setInvertedIndexColumns(Lists.newArrayList(STRING_COLUMN1)).setSortedColumn(LONG_COLUMN1)
            .setRangeIndexColumns(Lists.newArrayList(STRING_COLUMN2))
            .setNoDictionaryColumns(Lists.newArrayList(LONG_COLUMN2))
            .setVarLengthDictionaryColumns(Lists.newArrayList(STRING_COLUMN3))
            .setOnHeapDictionaryColumns(Lists.newArrayList(LONG_COLUMN3)).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(STRING_COLUMN1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, FieldSpec.DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, FieldSpec.DataType.INT).addMetric(LONG_COLUMN4, FieldSpec.DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    String tableNameWithType = tableConfig.getTableName();
    String segmentName = "testTable__0__0__123456";
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
            .setStreamName(tableNameWithType).setSchema(schema).setTimeColumnName(DATE_TIME_COLUMN).setCapacity(1000)
            .setAvgNumMultiValues(3).setNoDictionaryColumns(Sets.newHashSet(LONG_COLUMN2))
            .setVarLengthDictionaryColumns(Sets.newHashSet(STRING_COLUMN3))
            .setInvertedIndexColumns(Sets.newHashSet(STRING_COLUMN1))
            .setSegmentZKMetadata(getSegmentZKMetadata(segmentName)).setOffHeap(true)
            .setMemoryManager(new DirectMemoryManager(segmentName))
            .setStatsHistory(RealtimeSegmentStatsHistory.deserialzeFrom(new File(tmpDir, "stats")))
            .setConsumerDir(new File(tmpDir, "consumerDir").getAbsolutePath());

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);

    File outputDir = new File(tmpDir, "outputDir");
    RealtimeSegmentConverter converter =
        new RealtimeSegmentConverter(mutableSegmentImpl, outputDir.getAbsolutePath(), schema, tableNameWithType,
            tableConfig, segmentName, indexingConfig.getSortedColumn().get(0), indexingConfig.getInvertedIndexColumns(),
            null, null, indexingConfig.getNoDictionaryColumns(), indexingConfig.getVarLengthDictionaryColumns(), false);

    converter.build(SegmentVersion.v3, null);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(new File(outputDir, segmentName));
    Assert.assertEquals(metadata.getTotalDocs(), 0);
    Assert.assertEquals(metadata.getTimeColumn(), DATE_TIME_COLUMN);
    Assert.assertEquals(metadata.getTimeUnit(), TimeUnit.MILLISECONDS);
    Assert.assertEquals(metadata.getStartTime(), metadata.getEndTime());
    Assert.assertTrue(metadata.getAllColumns().containsAll(schema.getColumnNames()));
  }

  private SegmentZKMetadata getSegmentZKMetadata(String segmentName) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
    segmentZKMetadata.setCreationTime(System.currentTimeMillis());
    return segmentZKMetadata;
  }

  public void destroy() {
    FileUtils.deleteQuietly(TMP_DIR);
  }
}
