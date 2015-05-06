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
package com.linkedin.pinot.core.realtime.utils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;
import com.linkedin.pinot.core.realtime.TestRealtimeFileBasedReader;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.realtime.impl.dictionary.RealtimeDictionaryProvider;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


public class TestDimensionsAndMetricsSerDe {
  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static String filePath;
  private static Map<String, FieldType> fieldTypeMap;
  private static Schema schema;
  private static AvroRecordReader reader = null;

  @BeforeClass
  public static void before() throws Exception {
    filePath = TestRealtimeFileBasedReader.class.getClassLoader().getResource(AVRO_DATA).getFile();

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
    resetReader();
  }

  public static void resetReader() throws Exception {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    reader = new AvroRecordReader(FieldExtractorFactory.getPlainFieldExtractor(schema), filePath);
    reader.init();

  }

  @Test(enabled = false)
  public void testDimensionSerDe() throws Exception {
    Map<String, MutableDictionaryReader> dictionaryMap = new HashMap<String, MutableDictionaryReader>();
    for (String column : schema.getDimensionNames()) {
      dictionaryMap.put(column, RealtimeDictionaryProvider.getDictionaryFor(schema.getFieldSpecFor(column)));
    }

    RealtimeDimensionsSerDe dimSerDe = new RealtimeDimensionsSerDe(schema.getDimensionNames(), schema, dictionaryMap);

    while (reader.hasNext()) {
      GenericRow row = reader.next();

      for (String dimension : schema.getDimensionNames()) {

        dictionaryMap.get(dimension).index(row.getValue(dimension));
        Object val = row.getValue(dimension);
        int[] dicIds;
        if (val instanceof Object[]) {
          Object[] vals = (Object[]) val;
          for (int i = 0; i < vals.length; i++) {
            int dicId = dictionaryMap.get(dimension).indexOf(vals[i]);
            Assert.assertEquals(vals[i], dictionaryMap.get(dimension).get(dicId));
          }
        } else {
          int dicId = dictionaryMap.get(dimension).indexOf(val);
          if (row.getValue(dimension) == null) {
            Assert.assertEquals(dictionaryMap.get(dimension).get(dicId), "null");
          } else {
            Assert.assertEquals(dictionaryMap.get(dimension).get(dicId), row.getValue(dimension));
          }
        }
      }

      ByteBuffer serialized = dimSerDe.serialize(row);

      GenericRow deSerializedRow = dimSerDe.deSerialize(serialized);

      for (String dimension : schema.getDimensionNames()) {
        if (schema.getFieldSpecFor(dimension).isSingleValueField()) {
          if (row.getValue(dimension) == null) {
            Assert.assertEquals(deSerializedRow.getValue(dimension), "null");
          } else {
            Assert.assertEquals(deSerializedRow.getValue(dimension), row.getValue(dimension));
          }
        } else {
          Object[] incoming = (Object[]) row.getValue(dimension);
          Object[] outgoing = (Object[]) deSerializedRow.getValue(dimension);
          Assert.assertEquals(incoming.length, outgoing.length);
          Assert.assertEquals(incoming, outgoing);
        }
      }

    }
    reader.close();
  }

  @Test
  public void testMetricsSerDe() throws Exception {
    resetReader();

    RealtimeMetricsSerDe metricsSerDe = new RealtimeMetricsSerDe(schema);

    while (reader.hasNext()) {
      GenericRow row = reader.next();
      ByteBuffer buffer = metricsSerDe.serialize(row);

      for (String metric : schema.getMetricNames()) {
        Assert.assertEquals(row.getValue(metric), metricsSerDe.getRawValueFor(metric, buffer));
      }
    }
  }
}
