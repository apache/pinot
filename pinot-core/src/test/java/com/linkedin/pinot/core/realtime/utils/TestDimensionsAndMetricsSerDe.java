package com.linkedin.pinot.core.realtime.utils;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
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
    fieldTypeMap.put("viewerId", FieldType.dimension);
    fieldTypeMap.put("vieweeId", FieldType.dimension);
    fieldTypeMap.put("viewerPrivacySetting", FieldType.dimension);
    fieldTypeMap.put("vieweePrivacySetting", FieldType.dimension);
    fieldTypeMap.put("viewerObfuscationType", FieldType.dimension);
    fieldTypeMap.put("viewerCompanies", FieldType.dimension);
    fieldTypeMap.put("viewerOccupations", FieldType.dimension);
    fieldTypeMap.put("viewerRegionCode", FieldType.dimension);
    fieldTypeMap.put("viewerIndustry", FieldType.dimension);
    fieldTypeMap.put("viewerSchool", FieldType.dimension);
    fieldTypeMap.put("weeksSinceEpochSunday", FieldType.dimension);
    fieldTypeMap.put("daysSinceEpoch", FieldType.dimension);
    fieldTypeMap.put("minutesSinceEpoch", FieldType.time);
    fieldTypeMap.put("count", FieldType.metric);
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

  @Test
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
          Assert.assertEquals(row.getValue(dimension), dictionaryMap.get(dimension).get(dicId));
        }
      }

      IntBuffer serialized = dimSerDe.serializeToIntBuffer(row);

      GenericRow deSerializedRow = dimSerDe.deSerialize(serialized);

      for (String dimension : schema.getDimensionNames()) {
        if (schema.getFieldSpecFor(dimension).isSingleValueField()) {
          Assert.assertEquals(row.getValue(dimension), deSerializedRow.getValue(dimension));
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
