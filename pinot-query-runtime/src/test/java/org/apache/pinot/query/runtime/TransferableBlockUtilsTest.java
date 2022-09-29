package org.apache.pinot.query.runtime;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;


public class TransferableBlockUtilsTest {
  // TODO: Consolidate the test utils with DatablockTestUtils
  private static final Random RANDOM = new Random();
  private static final int ARRAY_SIZE = 5;
  private static final int TEST_ROW_COUNT = 5;
  private static final List<DataSchema.ColumnDataType> EXCLUDE_DATA_TYPES =
      ImmutableList.of(DataSchema.ColumnDataType.OBJECT, DataSchema.ColumnDataType.BYTES_ARRAY);

  private static DataSchema getDataSchema() {
    DataSchema.ColumnDataType[] allDataTypes = DataSchema.ColumnDataType.values();
    List<DataSchema.ColumnDataType> columnDataTypes = new ArrayList<DataSchema.ColumnDataType>();
    List<String> columnNames = new ArrayList<String>();
    for (int i = 0; i < allDataTypes.length; i++) {
      if (!EXCLUDE_DATA_TYPES.contains(allDataTypes[i])) {
        columnNames.add(allDataTypes[i].name());
        columnDataTypes.add(allDataTypes[i]);
      }
    }
    return new DataSchema(columnNames.toArray(new String[0]),
        columnDataTypes.toArray(new DataSchema.ColumnDataType[0]));
  }

  public static boolean randomlySettingNull(int percentile) {
    return RANDOM.nextInt(100) >= (100 - percentile);
  }

  public static List<Object[]> getRandomRows(DataSchema dataSchema, int numRows, int nullPercentile) {
    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      rows.add(getRandomRow(dataSchema, nullPercentile));
    }
    return rows;
  }

  public static Object[] getRandomRow(DataSchema dataSchema, int nullPercentile) {
    final int numColumns = dataSchema.getColumnNames().length;
    DataSchema.ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    Object[] row = new Object[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      switch (columnDataTypes[colId].getStoredType()) {
        case INT:
          row[colId] = RANDOM.nextInt();
          break;
        case LONG:
          row[colId] = RANDOM.nextLong();
          break;
        case FLOAT:
          row[colId] = RANDOM.nextFloat();
          break;
        case DOUBLE:
          row[colId] = RANDOM.nextDouble();
          break;
        case BIG_DECIMAL:
          row[colId] = BigDecimal.valueOf(RANDOM.nextDouble());
          break;
        case STRING:
          row[colId] = RandomStringUtils.random(RANDOM.nextInt(20));
          break;
        case BYTES:
          row[colId] = new ByteArray(RandomStringUtils.random(RANDOM.nextInt(20)).getBytes());
          break;
        // Just test Double here, all object types will be covered in ObjectCustomSerDeTest.
        case OBJECT:
          row[colId] = RANDOM.nextDouble();
          break;
        case BOOLEAN_ARRAY:
        case INT_ARRAY:
          int length = RANDOM.nextInt(ARRAY_SIZE);
          int[] intArray = new int[length];
          for (int i = 0; i < length; i++) {
            intArray[i] = RANDOM.nextInt();
          }
          row[colId] = intArray;
          break;
        case TIMESTAMP_ARRAY:
        case LONG_ARRAY:
          length = RANDOM.nextInt(ARRAY_SIZE);
          long[] longArray = new long[length];
          for (int i = 0; i < length; i++) {
            longArray[i] = RANDOM.nextLong();
          }
          row[colId] = longArray;
          break;
        case FLOAT_ARRAY:
          length = RANDOM.nextInt(ARRAY_SIZE);
          float[] floatArray = new float[length];
          for (int i = 0; i < length; i++) {
            floatArray[i] = RANDOM.nextFloat();
          }
          row[colId] = floatArray;
          break;
        case DOUBLE_ARRAY:
          length = RANDOM.nextInt(ARRAY_SIZE);
          double[] doubleArray = new double[length];
          for (int i = 0; i < length; i++) {
            doubleArray[i] = RANDOM.nextDouble();
          }
          row[colId] = doubleArray;
          break;
        case STRING_ARRAY:
          length = RANDOM.nextInt(ARRAY_SIZE);
          String[] stringArray = new String[length];
          for (int i = 0; i < length; i++) {
            stringArray[i] = RandomStringUtils.random(RANDOM.nextInt(20));
          }
          row[colId] = stringArray;
          break;
        default:
          throw new UnsupportedOperationException("Can't fill random data for column type: " + columnDataTypes[colId]);
      }
      // randomly set some entry to null
      if (columnDataTypes[colId].getStoredType() != DataSchema.ColumnDataType.OBJECT) {
        row[colId] = randomlySettingNull(nullPercentile) ? null : row[colId];
      }
    }
    return row;
  }

  public static List<Object[]> convertColumnar(DataSchema dataSchema, List<Object[]> rows) {
    final int numRows = rows.size();
    final int numColumns = dataSchema.getColumnNames().length;
    List<Object[]> columnars = new ArrayList<>(numColumns);
    for (int colId = 0; colId < numColumns; colId++) {
      columnars.add(new Object[numRows]);
      for (int rowId = 0; rowId < numRows; rowId++) {
        columnars.get(colId)[rowId] = rows.get(rowId)[colId];
      }
    }
    return columnars;
  }

  // Test that we only send one block when row size is greater than maxBlockSize.
  @Test
  public void testSplitBlockRowSizeGreaterThanMaxSize()
      throws Exception {
    DataSchema dataSchema = getDataSchema();
    List<Object[]> rows = getRandomRows(dataSchema, TEST_ROW_COUNT, 1);
    RowDataBlock rowBlock = DataBlockBuilder.buildFromRows(rows, dataSchema);
    List<BaseDataBlock> result = TransferableBlockUtils.getDataBlockChunks(rowBlock, BaseDataBlock.Type.ROW, 1);
    Assert.assertEquals(result.size(), TEST_ROW_COUNT);
  }

  // Test that we only send one block when block size is smaller than maxBlockSize.
  @Test
  public void testMaxSizeGreaterThanBlockSize()
      throws Exception {
    DataSchema dataSchema = getDataSchema();
    List<Object[]> rows = getRandomRows(dataSchema, TEST_ROW_COUNT, 1);
    RowDataBlock rowBlock = DataBlockBuilder.buildFromRows(rows, dataSchema);
    List<BaseDataBlock> result =
        TransferableBlockUtils.getDataBlockChunks(rowBlock, BaseDataBlock.Type.ROW, 4 * 1024 * 1024);
    Assert.assertEquals(result.size(), 1);
  }

  // Test that METADATA block is not supported.
  @Test
  public void testUnsupportedTypeMetadata()
      throws Exception {
    assertThrows(UnsupportedOperationException.class,
        () -> TransferableBlockUtils.getDataBlockChunks(new MetadataBlock(), BaseDataBlock.Type.METADATA,
            4 * 1024 * 1024));
  }

  // Test that we send the original block when block type is not supported: COLUMNAR.
  @Test
  public void testUnsupportedTypeColumnar()
      throws Exception {
    DataSchema dataSchema = getDataSchema();
    List<Object[]> columnars = convertColumnar(dataSchema, getRandomRows(dataSchema, TEST_ROW_COUNT, 1));
    ColumnarDataBlock columnarBlock = DataBlockBuilder.buildFromColumns(columnars, dataSchema);
    List<BaseDataBlock> result =
        TransferableBlockUtils.getDataBlockChunks(columnarBlock, BaseDataBlock.Type.COLUMNAR, 1);
    Assert.assertEquals(result.size(), 1);
  }

  // Test that we split the block based on maxBlockSize.
  @Test
  public void testSplitBlock()
      throws Exception {
    DataSchema dataSchema = getDataSchema();
    List<Object[]> rows = getRandomRows(dataSchema, TEST_ROW_COUNT, 1);
    RowDataBlock rowBlock = DataBlockBuilder.buildFromRows(rows, dataSchema);
    int[] columnOffsets = new int[rowBlock.getDataSchema().size()];
    int rowSizeInBytes = DataBlockUtils.computeColumnOffsets(dataSchema, columnOffsets);

    List<BaseDataBlock> result =
        TransferableBlockUtils.getDataBlockChunks(rowBlock, BaseDataBlock.Type.ROW, rowSizeInBytes * 2 + 1);
    Assert.assertEquals(result.size(), (int) Math.ceil((double) TEST_ROW_COUNT / 2));
  }
}
