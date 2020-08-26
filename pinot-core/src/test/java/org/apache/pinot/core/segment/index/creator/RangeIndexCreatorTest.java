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


import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.pinot.core.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.core.segment.index.readers.RangeIndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
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
        testDataType(FieldSpec.DataType.INT);
    }

    @Test
    public void testLong()
            throws Exception {
        testDataType(FieldSpec.DataType.LONG);
    }

    @Test
    public void testFloat()
            throws Exception {
        testDataType(FieldSpec.DataType.FLOAT);
    }

    @Test
    public void testDouble()
            throws Exception {
        testDataType(FieldSpec.DataType.DOUBLE);
    }

    @Test
    public void testIntMV()
            throws Exception {
        testDataTypeMV(FieldSpec.DataType.INT);
    }

    @Test
    public void testLongMV()
            throws Exception {
        testDataTypeMV(FieldSpec.DataType.LONG);
    }

    @Test
    public void testFloatMV()
            throws Exception {
        testDataTypeMV(FieldSpec.DataType.FLOAT);
    }

    @Test
    public void testDoubleMV()
            throws Exception {
        testDataTypeMV(FieldSpec.DataType.DOUBLE);
    }

    private void testDataType(FieldSpec.DataType dataType)
            throws IOException {
        File indexDir = new File(System.getProperty("java.io.tmpdir") + "/testRangeIndex");
        indexDir.mkdirs();
        FieldSpec fieldSpec = new MetricFieldSpec();
        fieldSpec.setDataType(dataType);
        String columnName = "latency";
        fieldSpec.setName(columnName);
        int cardinality = 20;
        int numDocs = 1000;
        int numValues = 1000;
        RangeIndexCreator creator =
                new RangeIndexCreator(indexDir, fieldSpec, dataType, -1, -1, numDocs, numValues);
        Random r = new Random();
        Number[] values = new Number[numValues];
        addDataToIndexer(dataType, cardinality, numDocs, 1, creator, r, values);


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
                checkValueForDocId(dataType, values, rangeStartArray, rangeId, docId, 1);
            }
        }
    }

    private void testDataTypeMV(FieldSpec.DataType dataType)
            throws IOException {
        File indexDir = new File(System.getProperty("java.io.tmpdir") + "/testRangeIndex");
        indexDir.mkdirs();
        FieldSpec fieldSpec = new DimensionFieldSpec();
        fieldSpec.setDataType(dataType);
        String columnName = "latency";
        fieldSpec.setName(columnName);
        fieldSpec.setSingleValueField(false);
        int cardinality = 20;
        int numDocs = 1000;
        int numValues = 1000;
        int numValuesPerColumn = 10;
        RangeIndexCreator creator =
                new RangeIndexCreator(indexDir, fieldSpec, dataType, -1, -1, numDocs, numValues * numValuesPerColumn);
        Random r = new Random();
        Number[] values = new Number[numValues * numValuesPerColumn];
        addDataToIndexer(dataType, cardinality, numDocs, numValuesPerColumn, creator, r, values);


        creator.seal();

        File rangeIndexFile = new File(indexDir, columnName + BITMAP_RANGE_INDEX_FILE_EXTENSION);
        //TEST THE BUFFER FORMAT

        //testRangeIndexBufferFormat(values, rangeIndexFile);

        //TEST USING THE READER
        PinotDataBuffer pinotDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(rangeIndexFile);
        RangeIndexReader rangeIndexReader = new RangeIndexReader(pinotDataBuffer);
        Number[] rangeStartArray = rangeIndexReader.getRangeStartArray();
        for (int rangeId = 0; rangeId < rangeStartArray.length; rangeId++) {
            ImmutableRoaringBitmap bitmap = rangeIndexReader.getDocIds(rangeId);
            for (int docId : bitmap.toArray()) {
                checkValueForDocId(dataType, values, rangeStartArray, rangeId, docId, numValuesPerColumn);
            }
        }
    }

    private void checkValueForDocId(FieldSpec.DataType dataType, Number[] values, Number[] rangeStartArray, int rangeId, int docId, int numValuesPerColumn) {
        switch (dataType) {
            case INT: {
                if (numValuesPerColumn > 1) {
                    checkIntMV(rangeStartArray, rangeId, values, docId, numValuesPerColumn);
                } else {
                    checkInt(rangeStartArray, rangeId, values, docId);
                }
                break;
            }
            case LONG: {
                if (numValuesPerColumn > 1) {
                    checkLongMV(rangeStartArray, rangeId, values, docId, numValuesPerColumn);
                } else {
                    checkLong(rangeStartArray, rangeId, values, docId);
                }
                break;
            }
            case FLOAT: {
                if (numValuesPerColumn > 1) {
                    checkFloatMV(rangeStartArray, rangeId, values, docId, numValuesPerColumn);
                } else {
                    checkFloat(rangeStartArray, rangeId, values, docId);
                }
                break;
            }
            case DOUBLE: {
                if (numValuesPerColumn > 1) {
                    checkDoubleMV(rangeStartArray, rangeId, values, docId, numValuesPerColumn);
                } else {
                    checkDouble(rangeStartArray, rangeId, values, docId);
                }
                break;
            }
        }
    }

    private void addDataToIndexer(FieldSpec.DataType dataType, int cardinality, int numDocs, int numValuesPerColumn, RangeIndexCreator creator, Random r, Number[] values) {
        switch (dataType) {
            case INT: {
                for (int i = 0; i < numDocs; i++) {

                    if (numValuesPerColumn > 1) {
                        int[] mvColumnArr = new int[numValuesPerColumn];
                        for (int j = 0; j < numValuesPerColumn; j++) {
                            int val = r.nextInt(cardinality);
                            mvColumnArr[j] = val;
                            values[numValuesPerColumn * i + j] = val;
                        }
                        creator.add(mvColumnArr, numValuesPerColumn);
                    } else {
                        int val = r.nextInt(cardinality);
                        creator.add(val);
                        values[i] = val;
                    }
                }
                break;
            }

            case LONG: {
                for (int i = 0; i < numDocs; i++) {
                    if (numValuesPerColumn > 1) {
                        long[] mvColumnArr = new long[numValuesPerColumn];
                        for (int j = 0; j < numValuesPerColumn; j++) {
                            long val = r.nextLong();
                            mvColumnArr[j] = val;
                            values[numValuesPerColumn * i + j] = val;
                        }
                        creator.add(mvColumnArr, numValuesPerColumn);
                    } else {
                        long val = r.nextLong();
                        creator.add(val);
                        values[i] = val;
                    }
                }
                break;
            }

            case FLOAT: {
                for (int i = 0; i < numDocs; i++) {
                    if (numValuesPerColumn > 1) {
                        float[] mvColumnArr = new float[numValuesPerColumn];
                        for (int j = 0; j < numValuesPerColumn; j++) {
                            float val = r.nextFloat();
                            mvColumnArr[j] = val;
                            values[numValuesPerColumn * i + j] = val;
                        }
                        creator.add(mvColumnArr, numValuesPerColumn);
                    } else {
                        float val = r.nextFloat();
                        creator.add(val);
                        values[i] = val;
                    }
                }
                break;
            }

            case DOUBLE: {
                for (int i = 0; i < numDocs; i++) {
                    if (numValuesPerColumn > 1) {
                        double[] mvColumnArr = new double[numValuesPerColumn];
                        for (int j = 0; j < numValuesPerColumn; j++) {
                            double val = r.nextDouble();
                            mvColumnArr[j] = val;
                            values[numValuesPerColumn * i + j] = val;
                        }
                        creator.add(mvColumnArr, numValuesPerColumn);
                    } else {
                        double val = r.nextDouble();
                        creator.add(val);
                        values[i] = val;
                    }
                }
                break;
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

    private void checkIntMV(Number[] rangeStartArray, int rangeId, Number[] values, int docId, int numValuesPerColumn) {
        boolean atleastOneInRange = false;

        if (rangeId != rangeStartArray.length - 1) {
            for (int i = 0; i < numValuesPerColumn; i++) {
                atleastOneInRange = atleastOneInRange || (rangeStartArray[rangeId].intValue() <= values[numValuesPerColumn * docId + i].intValue() && values[numValuesPerColumn * docId + i].intValue() < rangeStartArray[
                        rangeId + 1].intValue());
            }
        } else {
            for (int i = 0; i < numValuesPerColumn; i++) {
                atleastOneInRange = atleastOneInRange || (rangeStartArray[rangeId].intValue() <= values[numValuesPerColumn * docId + i].intValue());
            }
        }

        Assert.assertTrue(atleastOneInRange, "rangestart:" + rangeStartArray[rangeId]);
    }

    private void checkLong(Number[] rangeStartArray, int rangeId, Number[] values, int docId) {
        if (rangeId != rangeStartArray.length - 1) {
            Assert.assertTrue(
                    rangeStartArray[rangeId].longValue() <= values[docId].longValue() && values[docId].longValue() < rangeStartArray[
                            rangeId + 1].longValue(), "rangestart:" + rangeStartArray[rangeId] + " value:" + values[docId]);
        } else {
            Assert.assertTrue(rangeStartArray[rangeId].longValue() <= values[docId].longValue(),
                    "rangestart:" + rangeStartArray[rangeId] + " value:" + values[docId]);
        }
    }

    private void checkLongMV(Number[] rangeStartArray, int rangeId, Number[] values, int docId, int numValuesPerColumn) {
        boolean atleastOneInRange = false;

        if (rangeId != rangeStartArray.length - 1) {
            for (int i = 0; i < numValuesPerColumn; i++) {
                atleastOneInRange = atleastOneInRange || (rangeStartArray[rangeId].longValue() <= values[numValuesPerColumn * docId + i].longValue() && values[numValuesPerColumn * docId + i].longValue() < rangeStartArray[
                        rangeId + 1].longValue());
            }
        } else {
            for (int i = 0; i < numValuesPerColumn; i++) {
                atleastOneInRange = atleastOneInRange || (rangeStartArray[rangeId].longValue() <= values[numValuesPerColumn * docId + i].longValue());
            }
        }

        Assert.assertTrue(atleastOneInRange, "rangestart:" + rangeStartArray[rangeId]);
    }

    private void checkFloat(Number[] rangeStartArray, int rangeId, Number[] values, int docId) {
        if (rangeId != rangeStartArray.length - 1) {
            Assert.assertTrue(
                    rangeStartArray[rangeId].floatValue() <= values[docId].floatValue() && values[docId].floatValue() < rangeStartArray[
                            rangeId + 1].floatValue(), "rangestart:" + rangeStartArray[rangeId] + " value:" + values[docId]);
        } else {
            Assert.assertTrue(rangeStartArray[rangeId].floatValue() <= values[docId].floatValue(),
                    "rangestart:" + rangeStartArray[rangeId] + " value:" + values[docId]);
        }
    }

    private void checkFloatMV(Number[] rangeStartArray, int rangeId, Number[] values, int docId, int numValuesPerColumn) {
        boolean atleastOneInRange = false;

        if (rangeId != rangeStartArray.length - 1) {
            for (int i = 0; i < numValuesPerColumn; i++) {
                atleastOneInRange = atleastOneInRange || (rangeStartArray[rangeId].floatValue() <= values[numValuesPerColumn * docId + i].floatValue() && values[numValuesPerColumn * docId + i].floatValue() < rangeStartArray[
                        rangeId + 1].floatValue());
            }
        } else {
            for (int i = 0; i < numValuesPerColumn; i++) {
                atleastOneInRange = atleastOneInRange || (rangeStartArray[rangeId].floatValue() <= values[numValuesPerColumn * docId + i].floatValue());
            }
        }

        Assert.assertTrue(atleastOneInRange, "rangestart:" + rangeStartArray[rangeId]);
    }

    private void checkDouble(Number[] rangeStartArray, int rangeId, Number[] values, int docId) {
        if (rangeId != rangeStartArray.length - 1) {
            Assert.assertTrue(
                    rangeStartArray[rangeId].doubleValue() <= values[docId].doubleValue() && values[docId].doubleValue() < rangeStartArray[
                            rangeId + 1].doubleValue(), "rangestart:" + rangeStartArray[rangeId] + " value:" + values[docId]);
        } else {
            Assert.assertTrue(rangeStartArray[rangeId].doubleValue() <= values[docId].doubleValue(),
                    "rangestart:" + rangeStartArray[rangeId] + " value:" + values[docId]);
        }
    }

    private void checkDoubleMV(Number[] rangeStartArray, int rangeId, Number[] values, int docId, int numValuesPerColumn) {
        boolean atleastOneInRange = false;

        if (rangeId != rangeStartArray.length - 1) {
            for (int i = 0; i < numValuesPerColumn; i++) {
                atleastOneInRange = atleastOneInRange || (rangeStartArray[rangeId].doubleValue() <= values[numValuesPerColumn * docId + i].doubleValue() && values[numValuesPerColumn * docId + i].doubleValue() < rangeStartArray[
                        rangeId + 1].doubleValue());
            }
        } else {
            for (int i = 0; i < numValuesPerColumn; i++) {
                atleastOneInRange = atleastOneInRange || (rangeStartArray[rangeId].doubleValue() <= values[numValuesPerColumn * docId + i].doubleValue());
            }
        }

        Assert.assertTrue(atleastOneInRange, "rangestart:" + rangeStartArray[rangeId]);
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
                checkValueForDocId(dataType, values, rangeStart, i, docId, 1);
            }
        }
    }
}