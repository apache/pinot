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
package com.linkedin.pinot.core.startree.v2;

import java.io.File;
import java.util.List;
import java.util.Arrays;
import org.testng.Assert;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.io.IOException;
import com.google.common.io.Files;
import org.testng.annotations.Test;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.startree.hll.HllConstants;
import com.clearspring.analytics.stream.quantile.TDigest;
import com.linkedin.pinot.core.common.datatable.ObjectType;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.segment.memory.PinotByteBuffer;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.segment.creator.SingleValueRawIndexCreator;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.query.aggregation.function.customobject.QuantileDigest;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;


public class OnHeapStarTreeV2HelperTest {

  public List<Record> createData() {
    List<Record> data = new ArrayList<>();

    Record r1 = new Record();
    r1.setDimensionValues(new int[]{4, 1, 0});
    r1.setMetricValues(Arrays.asList(1, 2, 3, 1));
    data.add(r1);

    Record r2 = new Record();
    r2.setDimensionValues(new int[]{5, 1, 1});
    r2.setMetricValues(Arrays.asList(3, 2, 1, 1));
    data.add(r2);

    Record r3 = new Record();
    r3.setDimensionValues(new int[]{1, 2, 1});
    r3.setMetricValues(Arrays.asList(1, 2, 3, 1));
    data.add(r3);

    Record r4 = new Record();
    r4.setDimensionValues(new int[]{1, 3, 1});
    r4.setMetricValues(Arrays.asList(3, 2, 1, 1));
    data.add(r4);

    Record r5 = new Record();
    r5.setDimensionValues(new int[]{1, 1, 2});
    r5.setMetricValues(Arrays.asList(1, 2, 3, 1));
    data.add(r5);

    Record r6 = new Record();
    r6.setDimensionValues(new int[]{4, 4, 3});
    r6.setMetricValues(Arrays.asList(3, 2, 1, 1));
    data.add(r6);

    return data;
  }

  public List<AggregationFunctionColumnPair> createMet2AggfuncPairs() {
    List<AggregationFunctionColumnPair> metric2aggFuncPairs = new ArrayList<>();

    AggregationFunctionColumnPair pair1 = new AggregationFunctionColumnPair(AggregationFunctionType.SUM, "m1");
    metric2aggFuncPairs.add(pair1);
    AggregationFunctionColumnPair pair2 = new AggregationFunctionColumnPair(AggregationFunctionType.MIN, "m2");
    metric2aggFuncPairs.add(pair2);
    AggregationFunctionColumnPair pair3 = new AggregationFunctionColumnPair(AggregationFunctionType.MAX, "m3");
    metric2aggFuncPairs.add(pair3);
    AggregationFunctionColumnPair pair4 = new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, "star");
    metric2aggFuncPairs.add(pair4);

    return metric2aggFuncPairs;
  }

  public List<Record> expectedSortedData() {
    List<Record> data = new ArrayList<>();

    Record r1 = new Record();
    r1.setDimensionValues(new int[]{1, 1, 2});
    r1.setMetricValues(Arrays.asList(1, 2, 3, 1));
    data.add(r1);

    Record r2 = new Record();
    r2.setDimensionValues(new int[]{1, 2, 1});
    r2.setMetricValues(Arrays.asList(1, 2, 3, 1));
    data.add(r2);

    Record r3 = new Record();
    r3.setDimensionValues(new int[]{1, 3, 1});
    r3.setMetricValues(Arrays.asList(3, 2, 1, 1));
    data.add(r3);

    Record r4 = new Record();
    r4.setDimensionValues(new int[]{4, 4, 3});
    r4.setMetricValues(Arrays.asList(3, 2, 1, 1));
    data.add(r4);

    return data;
  }

  public List<Record> expectedFilteredData() {
    List<Record> data = new ArrayList<>();

    Record r1 = new Record();
    r1.setDimensionValues(new int[]{1, -1, 1});
    r1.setMetricValues(Arrays.asList(1, 2, 3, 1));
    data.add(r1);

    Record r2 = new Record();
    r2.setDimensionValues(new int[]{1, -1, 1});
    r2.setMetricValues(Arrays.asList(3, 2, 1, 1));
    data.add(r2);

    Record r3 = new Record();
    r3.setDimensionValues(new int[]{1, -1, 2});
    r3.setMetricValues(Arrays.asList(1, 2, 3, 1));
    data.add(r3);

    Record r4 = new Record();
    r4.setDimensionValues(new int[]{5, -1, 1});
    r4.setMetricValues(Arrays.asList(3, 2, 1, 1));
    data.add(r4);

    return data;
  }

  public List<Record> expectedCondensedData() {
    List<Record> data = new ArrayList<>();

    Record r1 = new Record();
    r1.setDimensionValues(new int[]{1, -1, 1});
    r1.setMetricValues(Arrays.asList(4.0, 2.0, 3.0, 2L));
    data.add(r1);

    Record r2 = new Record();
    r2.setDimensionValues(new int[]{1, -1, 2});
    r2.setMetricValues(Arrays.asList(1.0, 2.0, 3.0, 1L));
    data.add(r2);

    Record r3 = new Record();
    r3.setDimensionValues(new int[]{5, -1, 1});
    r3.setMetricValues(Arrays.asList(3.0, 2.0, 1.0, 1L));
    data.add(r3);

    return data;
  }

  @Test
  public void testSortStarTreeData() {
    List<Record> data = createData();
    OnHeapStarTreeV2Builder obj = new OnHeapStarTreeV2Builder();
    List<Record> actualData = obj.sortStarTreeData(2, 6, Arrays.asList(0, 1, 2), data);

    List<Record> expected = expectedSortedData();
    assertRecordsList(expected, actualData);
  }

  @Test
  public void testFilterData() {
    List<Record> data = createData();
    OnHeapStarTreeV2Builder obj = new OnHeapStarTreeV2Builder();
    List<Record> actualData = obj.filterData(1, 5, 1, Arrays.asList(0, 1, 2), data);

    List<Record> expected = expectedFilteredData();
    printRecordsList(expected);
    assertRecordsList(expected, actualData);
  }

  @Test
  public void testTDigest() throws IOException {
    int a = 10;
    byte[] dest;

    TDigest tDigest = new TDigest(100);
    tDigest.add(a);
    byte[] src = ObjectCustomSerDe.serialize(tDigest);
    TDigest obj = ObjectCustomSerDe.deserialize(src, ObjectType.TDigest);
    byte[] hola = ObjectCustomSerDe.serialize(obj);

    System.out.println(tDigest.byteSize());

    File temp = Files.createTempDir();
    SingleValueRawIndexCreator indexCreator =
        new SingleValueVarByteRawIndexCreator(temp, ChunkCompressorFactory.CompressionType.PASS_THROUGH, "random",
             1, tDigest.byteSize());
    indexCreator.index(0, src);
    indexCreator.close();

    File columnDataFile = new File(temp, "random.sv.raw.fwd");
    long size = columnDataFile.length();
    PinotDataBuffer buffer = PinotByteBuffer.mapFile(columnDataFile, false, 0, size, ByteOrder.BIG_ENDIAN,"star tree v2");
    StarTreeV2AggfunColumnPairDataSource
        source = new StarTreeV2AggfunColumnPairDataSource(buffer, "random", 1, FieldSpec.DataType.BYTES);

    Block b = source.getNextBlock();
    BlockValSet blockValSet = b.getBlockValueSet();
    BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();

    while (itr.hasNext()) {
      dest = itr.nextBytesVal();
      System.out.println("hola");
    }
  }

  @Test
  public void testTDigestRawData() throws IOException {

    int _maxLength = 0;
    TDigest tDigest = new TDigest(100);
    for (int i = 0; i < 5; i++) {
      tDigest.add(i);
      _maxLength = Math.max(tDigest.byteSize(), _maxLength);
    }
    byte[] src = ObjectCustomSerDe.serialize(tDigest);
    TDigest obj = ObjectCustomSerDe.deserialize(src, ObjectType.TDigest);
    byte[] hola = ObjectCustomSerDe.serialize(obj);

    System.out.println(tDigest.byteSize());

    File temp = Files.createTempDir();
    SingleValueRawIndexCreator indexCreator =
        new SingleValueVarByteRawIndexCreator(temp, ChunkCompressorFactory.CompressionType.PASS_THROUGH, "random",
            1, tDigest.byteSize());
    indexCreator.index(0, src);
    indexCreator.close();

    File columnDataFile = new File(temp, "random.sv.raw.fwd");
    long size = columnDataFile.length();

    PinotDataBuffer buffer = PinotByteBuffer.mapFile(columnDataFile, false, 0, size, ByteOrder.BIG_ENDIAN,"star tree v2");
    StarTreeV2AggfunColumnPairDataSource source = new StarTreeV2AggfunColumnPairDataSource(buffer, "random", 1, FieldSpec.DataType.BYTES);

    Block b = source.getNextBlock();
    BlockValSet blockValSet = b.getBlockValueSet();
    BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();

    while (itr.hasNext()) {
      byte[] dest = itr.nextBytesVal();
      System.out.println("hola");
    }

    return;
  }

  @Test
  public void testHLL() throws IOException {
    int a = 10;
    byte[] dest;

    HyperLogLog hyperLogLog = new HyperLogLog(HllConstants.DEFAULT_LOG2M);
    hyperLogLog.offer(a);
    byte[] src = ObjectCustomSerDe.serialize(hyperLogLog);
    HyperLogLog obj = ObjectCustomSerDe.deserialize(src, ObjectType.HyperLogLog);
    byte[] hola = ObjectCustomSerDe.serialize(obj);

    System.out.println(hyperLogLog.sizeof());
    System.out.println(hyperLogLog.getBytes().length);

    File temp = Files.createTempDir();
    SingleValueRawIndexCreator indexCreator =
        new SingleValueVarByteRawIndexCreator(temp, ChunkCompressorFactory.CompressionType.PASS_THROUGH, "random",
             1, hyperLogLog.getBytes().length);
    indexCreator.index(0, src);
    indexCreator.close();

    File columnDataFile = new File(temp, "random.sv.raw.fwd");
    long size = columnDataFile.length();

    PinotDataBuffer buffer = PinotByteBuffer.mapFile(columnDataFile, false, 0, size, ByteOrder.BIG_ENDIAN,"star tree v2");
    StarTreeV2AggfunColumnPairDataSource source = new StarTreeV2AggfunColumnPairDataSource(buffer, "random", 1, FieldSpec.DataType.BYTES);

    Block b = source.getNextBlock();
    BlockValSet blockValSet = b.getBlockValueSet();
    BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();

    while (itr.hasNext()) {
      dest = itr.nextBytesVal();
      System.out.println("Hola");
    }
  }

  @Test
  public void testHLLRawData() throws IOException {
    int _maxLength = 0;
    byte[] dest;

    HyperLogLog hyperLogLog = new HyperLogLog(HllConstants.DEFAULT_LOG2M);
    for (int i = 0; i < 5; i++) {
      hyperLogLog.offer(i);
      _maxLength = Math.max(hyperLogLog.getBytes().length, _maxLength);
    }

    byte[] src = ObjectCustomSerDe.serialize(hyperLogLog);
    HyperLogLog obj = ObjectCustomSerDe.deserialize(src, ObjectType.HyperLogLog);
    byte[] hola = ObjectCustomSerDe.serialize(obj);

    System.out.println(hyperLogLog.getBytes().length);
    System.out.println(hyperLogLog.sizeof());

    File temp = Files.createTempDir();
    SingleValueRawIndexCreator indexCreator =
        new SingleValueVarByteRawIndexCreator(temp, ChunkCompressorFactory.CompressionType.PASS_THROUGH, "random",
            1, hyperLogLog.getBytes().length);
    indexCreator.index(0, src);
    indexCreator.close();

    File columnDataFile = new File(temp, "random.sv.raw.fwd");
    long size = columnDataFile.length();

    PinotDataBuffer buffer = PinotByteBuffer.mapFile(columnDataFile, false, 0, size, ByteOrder.BIG_ENDIAN,"star tree v2");
    StarTreeV2AggfunColumnPairDataSource source = new StarTreeV2AggfunColumnPairDataSource(buffer, "random", 1, FieldSpec.DataType.BYTES);

    Block b = source.getNextBlock();
    BlockValSet blockValSet = b.getBlockValueSet();
    BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();

    while (itr.hasNext()) {
      dest = itr.nextBytesVal();
      System.out.println("hola");
    }

    return;
  }

  @Test
  public void testQDigest() throws IOException {
    byte[] dest;
    int _maxLength = 0;
    final double DEFAULT_MAX_ERROR = 0.05;

    QuantileDigest qDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
    for (int i = 0; i < 5; i++) {
      qDigest.add(i);
      _maxLength = Math.max(qDigest.estimatedSerializedSizeInBytes(), _maxLength);
    }

    byte[] src = ObjectCustomSerDe.serialize(qDigest);
    QuantileDigest obj = ObjectCustomSerDe.deserialize(src, ObjectType.QuantileDigest);
    byte[] hola = ObjectCustomSerDe.serialize(obj);

    System.out.println(qDigest.estimatedSerializedSizeInBytes());

    File temp = Files.createTempDir();
    SingleValueRawIndexCreator indexCreator =
        new SingleValueVarByteRawIndexCreator(temp, ChunkCompressorFactory.CompressionType.PASS_THROUGH, "random",
            1, qDigest.estimatedSerializedSizeInBytes());
    indexCreator.index(1, src);
    indexCreator.close();

    File columnDataFile = new File(temp, "random.sv.raw.fwd");
    long size = columnDataFile.length();
    PinotDataBuffer buffer = PinotByteBuffer.mapFile(columnDataFile, false, 0, size, ByteOrder.BIG_ENDIAN,"star tree v2");
    StarTreeV2AggfunColumnPairDataSource source = new StarTreeV2AggfunColumnPairDataSource(buffer, "random", 1, FieldSpec.DataType.BYTES);

    Block b = source.getNextBlock();
    BlockValSet blockValSet = b.getBlockValueSet();
    BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();

    while (itr.hasNext()) {
      dest = itr.nextBytesVal();
      System.out.println("hola");
    }

    return;
  }

  @Test
  public void testQDigestRawData() throws IOException {
    long a = 10L;
    byte[] dest;
    final double DEFAULT_MAX_ERROR = 0.05;
    QuantileDigest qDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
    qDigest.add(a);
    byte[] src = ObjectCustomSerDe.serialize(qDigest);
    QuantileDigest obj = ObjectCustomSerDe.deserialize(src, ObjectType.QuantileDigest);
    byte[] hola = ObjectCustomSerDe.serialize(obj);

    System.out.println(qDigest.estimatedSerializedSizeInBytes());

    File temp = Files.createTempDir();
    SingleValueRawIndexCreator indexCreator =
        new SingleValueVarByteRawIndexCreator(temp, ChunkCompressorFactory.CompressionType.PASS_THROUGH, "random",
             1, qDigest.estimatedSerializedSizeInBytes());
    indexCreator.index(1, src);
    indexCreator.close();

    File columnDataFile = new File(temp, "random.sv.raw.fwd");
    long size = columnDataFile.length();
    PinotDataBuffer buffer = PinotByteBuffer.mapFile(columnDataFile, false, 0, size, ByteOrder.BIG_ENDIAN,"star tree v2");
    StarTreeV2AggfunColumnPairDataSource source = new StarTreeV2AggfunColumnPairDataSource(buffer, "random", 1, FieldSpec.DataType.BYTES);

    Block b = source.getNextBlock();
    BlockValSet blockValSet = b.getBlockValueSet();
    BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();

    while (itr.hasNext()) {
      dest = itr.nextBytesVal();
    }

    return;
  }

  private void assertRecordsList(List<Record> expected, List<Record> actual) {
    if (expected.size() == 0) {
      Assert.assertEquals(actual.size(), expected.size());
      return;
    }

    for (int i = 0; i < expected.size(); i++) {
      Record expR = expected.get(i);
      Record actR = actual.get(i);
      assertRecord(expR, actR);
    }
  }

  private void assertRecord(Record a, Record b) {

    int aD[] = a.getDimensionValues();
    int bD[] = b.getDimensionValues();

    for (int i = 0; i < aD.length; i++) {
      Assert.assertEquals(aD[i], bD[i]);
    }

    List<Object> aM = a.getMetricValues();
    List<Object> bM = b.getMetricValues();

    for (int i = 0; i < aM.size(); i++) {
      Assert.assertEquals(aM.get(i), bM.get(i));
    }
  }

  private void printRecordsList(List<Record> records) {
    for (Record record : records) {
      int aD[] = record.getDimensionValues();
      for (int i = 0; i < aD.length; i++) {
        System.out.print(aD[i]);
        System.out.print(" ");
      }

      List<Object> aM = record.getMetricValues();
      for (int i = 0; i < aM.size(); i++) {
        System.out.print(aM.get(i));
        System.out.print(" ");
      }

      System.out.print("\n");
    }
  }
}