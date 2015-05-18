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
package com.linkedin.pinot.core.realtime;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.operator.filter.BitmapBasedFilterOperator;
import com.linkedin.pinot.core.realtime.impl.FileBasedStreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.FileBasedStreamProviderImpl;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


public class RealtimeSegmentTest {
  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static String filePath;
  private static Map<String, FieldType> fieldTypeMap;
  private static Schema schema;
  private static RealtimeSegment segment;

  @BeforeClass
  public static void before() throws Exception {
    filePath = RealtimeFileBasedReaderTest.class.getClassLoader().getResource(AVRO_DATA).getFile();
    fieldTypeMap = new HashMap<String, FieldSpec.FieldType>();
    fieldTypeMap.put("viewerId", FieldType.DIMENSION);
    fieldTypeMap.put("vieweeId", FieldType.DIMENSION);
    fieldTypeMap.put("viewerPrivacySetting", FieldType.DIMENSION);
    fieldTypeMap.put("vieweePrivacySetting", FieldType.DIMENSION);
    fieldTypeMap.put("viewerObfuscationType", FieldType.DIMENSION);
    fieldTypeMap.put("viewerCompanies", FieldType.DIMENSION);
    fieldTypeMap.put("viewerOccupations", FieldType.DIMENSION);
    fieldTypeMap.put("viewerRegionCode", FieldType.DIMENSION);
    fieldTypeMap.put("viewerIndustry", FieldType.DIMENSION);
    fieldTypeMap.put("viewerSchool", FieldType.DIMENSION);
    fieldTypeMap.put("weeksSinceEpochSunday", FieldType.DIMENSION);
    fieldTypeMap.put("daysSinceEpoch", FieldType.DIMENSION);
    fieldTypeMap.put("minutesSinceEpoch", FieldType.TIME);
    fieldTypeMap.put("count", FieldType.METRIC);
    schema = SegmentTestUtils.extractSchemaFromAvro(new File(filePath), fieldTypeMap, TimeUnit.MINUTES);

    StreamProviderConfig config = new FileBasedStreamProviderConfig(FileFormat.AVRO, filePath, schema);
    System.out.println(config);
    StreamProvider provider = new FileBasedStreamProviderImpl();
    provider.init(config);

    segment = new RealtimeSegmentImpl(schema, 100000);
    GenericRow row = provider.next();
    while (row != null) {
      segment.index(row);
      row = provider.next();
    }
    provider.shutdown();
  }

  @Test
  public void test1() throws Exception {
    DataSource ds = segment.getDataSource("viewerId");
    Block b = ds.nextBlock();
    BlockValSet set = b.getBlockValueSet();
    BlockSingleValIterator it = (BlockSingleValIterator) set.iterator();
    BlockMetadata metadata = b.getMetadata();
    while (it.next()) {
      int dicId = it.nextIntVal();
    }

  }

  @Test
  public void testMetricPredicate() throws Exception {
    DataSource ds1 = segment.getDataSource("count");

    BitmapBasedFilterOperator op = new BitmapBasedFilterOperator(ds1);
    List<String> rhs = new ArrayList<String>();
    rhs.add("1");
    Predicate predicate = new EqPredicate("count", rhs);
    op.setPredicate(predicate);

    Block b = op.nextBlock();
    BlockDocIdIterator iterator = b.getBlockDocIdSet().iterator();

    DataSource ds2 = segment.getDataSource("count");

    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) ds2.nextBlock().getBlockValueSet().iterator();
    int docId = iterator.next();
    int counter = 0;
    while (docId != Constants.EOF) {
      blockValIterator.skipTo(docId);
      Assert.assertEquals(ds1.getDictionary().get(blockValIterator.nextIntVal()), 1);
      docId = iterator.next();
      counter++;
    }
    Assert.assertEquals(counter, 100000);
  }

  @Test
  public void testNoMatchFilteringMetricPredicate() throws Exception {
    DataSource ds1 = segment.getDataSource("count");

    BitmapBasedFilterOperator op = new BitmapBasedFilterOperator(ds1);
    List<String> rhs = new ArrayList<String>();
    rhs.add("1");
    Predicate predicate = new NEqPredicate("count", rhs);

    op.setPredicate(predicate);
    Block b = op.nextBlock();
    BlockDocIdIterator iterator = b.getBlockDocIdSet().iterator();

    int counter = 0;
    int docId = iterator.next();
    while (docId != Constants.EOF) {
      // shouldn't reach here.
      Assert.assertTrue(false);
      docId = iterator.next();
      counter++;
    }
    Assert.assertEquals(counter, 0);
  }
}
