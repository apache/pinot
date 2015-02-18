package com.linkedin.pinot.core.realtime;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.realtime.impl.fwdindex.ByteBufferUtils;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;

public class TestByteBufferUtils {
  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static String filePath;
  private static Map<String, FieldType> fieldTypeMap;
  private static Schema schema;

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
  }

  @Test
  public void testDimensionAccess1() {
    List<List<Integer>> rawValueContainer = new LinkedList<List<Integer>>();
    List<ByteBuffer> dimbufferContainer = new LinkedList<ByteBuffer>();

    for (int i = 0; i < 1000; i++) {
      List<Integer> row = Lists.<Integer>newLinkedList();
      Random random = new Random(System.currentTimeMillis());
      for (String dimension : schema.getDimensions()) {
        if (schema.getFieldSpecFor(dimension).isSingleValueField()) {
          row.add(random.nextInt(10000));

        } else {
          int len = random.nextInt(10);
          row.add(len);
          for (int j =0; j < len; j++) {
            row.add(random.nextInt(10000));
          }
        }
      }
      rawValueContainer.add(row);
    }

    for (List<Integer> row : rawValueContainer) {
      ByteBuffer dimBuff = ByteBuffer.allocate(row.size() * 4);
      for (Integer e : row) {
        dimBuff.putInt(e);
      }
      dimbufferContainer.add(dimBuff);
    }

    Assert.assertEquals(rawValueContainer.size(), dimbufferContainer.size());

    for (int i = 0; i < dimbufferContainer.size(); i++) {
      int offset = 0;
      ByteBuffer buff = dimbufferContainer.get(i);
      buff.rewind();
      for (Integer e : rawValueContainer.get(i)) {
        Assert.assertEquals(e.intValue(), buff.getInt(offset));
        offset +=4;
      }
    }

    for (int i = 0; i < rawValueContainer.size(); i++) {
      List<Integer> rawRow = rawValueContainer.get(i);
      ByteBuffer byteBufferRow = dimbufferContainer.get(i);

      for (String dimension : schema.getDimensions()) {
        int [] valuesFromList = extractValuesFromList(dimension, rawRow);
        int [] valuesFromBuff = new int[] {};
        Assert.assertEquals(valuesFromBuff, valuesFromList);
      }
    }

  }

  private int[] extractValuesFromList(String dimString, List<Integer> row) {
    int listOffset = 0;


    for (String dimension : schema.getDimensions()) {
      if (dimension.equals(dimString)) {
        break;
      }

      listOffset += 1;

      if (!schema.getFieldSpecFor(dimension).isSingleValueField()) {
       listOffset += row.get(listOffset);
      }
    }

    if (schema.getFieldSpecFor(dimString).isSingleValueField()) {
      return new int[] {row.get(listOffset)};
    } else {
      int [] ret = new int[row.get(listOffset) + 1];
      int count = 0;
      for (int i = listOffset + 1; i <= listOffset + 1 + row.get(listOffset); i++ ) {
        ret[count] = row.get(i);
        count++;
      }
      return ret;
    }

  }
}
