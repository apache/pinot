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
package org.apache.pinot.core.segment.index.creator;

import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.pinot.core.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.core.segment.index.readers.RangeIndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.core.segment.creator.impl.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;


/**
 * Class for testing Range index.
 */
public class RangeIndexCreatorTest {

  @Test
  public void testInt()
      throws Exception {
    File indexDir = new File(System.getProperty("java.io.tmpdir") + "/testRangeIndex");
    indexDir.mkdirs();
    FieldSpec fieldSpec = new MetricFieldSpec();
    fieldSpec.setDataType(FieldSpec.DataType.INT);
    String columnName = "latency";
    fieldSpec.setName(columnName);
    int cardinality = 5;
    int numDocs = 50;
    int numValues = 50;
    RangeIndexCreator creator =
        new RangeIndexCreator(indexDir, fieldSpec, FieldSpec.DataType.INT, -1, -1, numDocs, numValues);
    Random r = new Random();
    Number[] values = new Number[numValues];
    for (int i = 0; i < numDocs; i++) {
      int val = r.nextInt(cardinality);
      creator.add(val);
      values[i] = val;
    }
    creator.seal();

    File rangeIndexFile = new File(indexDir, columnName + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    //TEST THE BUFFER FORMAT

    testRangeIndexBufferFormat(values, rangeIndexFile);

    //TEST USING THE READER
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(rangeIndexFile);
    RangeIndexReader rangeIndexReader = new RangeIndexReader(pinotDataBuffer);
    Number[] rangeStartArray = rangeIndexReader.getRangeStartArray();
    for (int rangeId = 0; rangeId < rangeStartArray.length; rangeId++) {
      ImmutableRoaringBitmap bitmap = rangeIndexReader.getDocIds(rangeId);
      for (int docId : bitmap.toArray()) {
        checkInt(rangeStartArray, rangeId, values, docId);
      }
    }
  }

  private void checkInt(Number[] rangeStartArray, int rangeId, Number[] values, int docId) {
    if (rangeId != rangeStartArray.length - 1) {
      Assert.assertTrue(
          rangeStartArray[rangeId].intValue() <= values[docId].intValue() && values[docId].intValue() < rangeStartArray[
              rangeId + 1].intValue(), "rangestart:" + rangeStartArray[rangeId] + " value:" + values[docId]);
    } else {
      Assert.assertTrue(rangeStartArray[rangeId].intValue() <= values[docId].intValue(),
          "rangestart:" + rangeStartArray[rangeId] + " value:" + values[docId]);
    }
  }

  private void testRangeIndexBufferFormat(Number[] values, File rangeIndexFile)
      throws IOException {
    DataInputStream dis = new DataInputStream(new FileInputStream(rangeIndexFile));
    int version = dis.readInt();
    int valueTypeBytesLength = dis.readInt();

    byte[] valueTypeBytes = new byte[valueTypeBytesLength];
    dis.read(valueTypeBytes);
    String name = new String(valueTypeBytes);
    FieldSpec.DataType dataType = FieldSpec.DataType.valueOf(name);

    int numRanges = dis.readInt();

    Number[] rangeStart = new Number[numRanges];
    Number rangeEnd;

    switch (dataType) {
      case INT:
        for (int i = 0; i < numRanges; i++) {
          rangeStart[i] = dis.readInt();
        }
        rangeEnd = dis.readInt();
        break;
      case LONG:
        for (int i = 0; i < numRanges; i++) {
          rangeStart[i] = dis.readLong();
        }
        rangeEnd = dis.readLong();
        break;
      case FLOAT:
        for (int i = 0; i < numRanges; i++) {
          rangeStart[i] = dis.readFloat();
        }
        rangeEnd = dis.readFloat();
        break;
      case DOUBLE:
        for (int i = 0; i < numRanges; i++) {
          rangeStart[i] = dis.readDouble();
        }
        rangeEnd = dis.readDouble();
        break;
    }

    long[] rangeBitmapOffsets = new long[numRanges + 1];
    for (int i = 0; i <= numRanges; i++) {
      rangeBitmapOffsets[i] = dis.readLong();
    }
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[numRanges];
    for (int i = 0; i < numRanges; i++) {
      long serializedBitmapLength;
      serializedBitmapLength = rangeBitmapOffsets[i + 1] - rangeBitmapOffsets[i];
      byte[] bytes = new byte[(int) serializedBitmapLength];
      dis.read(bytes, 0, (int) serializedBitmapLength);
      bitmaps[i] = new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes));
      for (int docId : bitmaps[i].toArray()) {
        if (i != numRanges - 1) {
          Assert.assertTrue(
              rangeStart[i].intValue() <= values[docId].intValue() && values[docId].intValue() < rangeStart[i + 1]
                  .intValue(), "rangestart:" + rangeStart[i] + " value:" + values[docId]);
        } else {
          Assert.assertTrue(rangeStart[i].intValue() <= values[docId].intValue());
        }
      }
    }
  }
}