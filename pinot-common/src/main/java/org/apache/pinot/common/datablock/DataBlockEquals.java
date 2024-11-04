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
package org.apache.pinot.common.datablock;

import java.util.Arrays;
import java.util.Objects;
import org.apache.pinot.common.utils.DataSchema;
import org.roaringbitmap.RoaringBitmap;


/**
 * A utility class used to compare two DataBlocks.
 */
public class DataBlockEquals {

  private DataBlockEquals() {
  }

  /**
   * Returns true if the two DataBlocks are of the same type, the same schema and contains the same data in the same
   * data according to the {@link DefaultContentComparator default contentComparator}.
   *
   * This means that a DataBlock of type {@link DataBlock.Type#COLUMNAR} and a DataBlock of type
   * {@link DataBlock.Type#ROW} will not be considered equal even if they contain the same data in the same order.
   */
  public static boolean equals(DataBlock left, DataBlock right) {
    return equals(left, right, DefaultContentComparator.INSTANCE);
  }

  /**
   * Returns true if the two DataBlocks are of the same type, the same schema and contains the same data in the same
   * data according to the given contentComparator.
   *
   * This means that a DataBlock of type {@link DataBlock.Type#COLUMNAR} and a DataBlock of type
   * {@link DataBlock.Type#ROW} will not be considered equal even if they contain the same data according to the
   * contentComparator.
   */
  public static boolean equals(DataBlock left, DataBlock right, ContentComparator contentComparator) {
    return sameType(left, right) && sameSchema(left, right) && sameContent(left, right, contentComparator);
  }

  /**
   * Returns true iff the two DataBlocks have the same type.
   *
   * This comparison is strict, meaning that a DataBlock of type {@link DataBlock.Type#COLUMNAR} and a DataBlock of type
   * {@link DataBlock.Type#ROW} will not be considered equal.
   */
  public static boolean sameType(DataBlock left, DataBlock right) {
    return left.getDataBlockType() == right.getDataBlockType();
  }

  /**
   * Returns true iff the two DataBlocks have the same schema.
   */
  public static boolean sameSchema(DataBlock left, DataBlock right) {
    return Objects.equals(left.getDataSchema(), right.getDataSchema());
  }

  /**
   * Returns true iff the two DataBlocks have the same content according to the
   * {@link DefaultContentComparator default contentComparator}.
   *
   * Contrary to {@link #equals(DataBlock, DataBlock)}, this method considers columnar and row types to be compatible.
   */
  public static boolean sameContent(DataBlock left, DataBlock right) {
    return sameContent(left, right, DefaultContentComparator.INSTANCE);
  }

  /**
   * Returns true iff the two DataBlocks have the same content according to the given contentComparator.
   *
   * Contrary to {@link #equals(DataBlock, DataBlock)}, this method considers columnar and row types to be compatible.
   */
  public static boolean sameContent(DataBlock left, DataBlock right, ContentComparator contentComparator) {
    return contentComparator.equals(left, right);
  }

  /**
   * Throws an {@link IllegalArgumentException} if the two DataBlocks are not equal according to the
   * {@link DefaultContentComparator default contentComparator}.
   *
   * The exception will contain a message describing the difference.
   */
  public static void checkSameContent(DataBlock left, DataBlock right, String message) {
    try {
      sameContent(left, right, DefaultContentComparator.CHECKER);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(message, e);
    }
  }

  /**
   * Throws an {@link IllegalArgumentException} if the two DataBlocks are not equal according to the given
   * contentComparator.
   *
   * It is encouraged to use this method with a custom contentComparator that provides a meaningful message in the
   * {@link ContentComparator#equals(DataBlock, DataBlock)} method by throwing an exception with a message that
   * describes the difference.
   * If the contentComparator does not throw an exception, this method will throw an exception with a generic message
   * when the two DataBlocks are not equal.
   *
   */
  public static void checkSameContent(DataBlock left, DataBlock right, String message,
      ContentComparator contentComparator) {
    try {
      if (!sameContent(left, right, contentComparator)) {
        throw new IllegalArgumentException(
            "Different content: " + left + " and " + right + ". Content comparator " + contentComparator.getClass()
                + " didn't notify the reason for the difference.");
      }
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(message, e);
    }
  }

  @FunctionalInterface
  public interface ContentComparator {
    boolean equals(DataBlock left, DataBlock right);

    default ContentComparator andThen(ContentComparator other) {
      return (left, right) -> equals(left, right) && other.equals(left, right);
    }
  }

  public static class DefaultContentComparator implements ContentComparator {
    private final boolean _failOnFalse;
    public static final DefaultContentComparator INSTANCE = new DefaultContentComparator(false);
    public static final DefaultContentComparator CHECKER = new DefaultContentComparator(true);

    private DefaultContentComparator(boolean failOnFalse) {
      _failOnFalse = failOnFalse;
    }

    @Override
    public boolean equals(DataBlock left, DataBlock right) {
      DataBlock.Type leftType = left.getDataBlockType();
      DataBlock.Type rightType = right.getDataBlockType();
      if (leftType == DataBlock.Type.METADATA) {
        if (rightType != DataBlock.Type.METADATA) {
          if (_failOnFalse) {
            throw new IllegalArgumentException("Different types: " + leftType + " and " + rightType);
          }
          return false;
        }
        if (!Objects.equals(left.getExceptions(), right.getExceptions())) {
          if (_failOnFalse) {
            throw new IllegalArgumentException("Different exceptions: " + left.getExceptions() + " and "
                + right.getExceptions());
          }
          return false;
        }
        if (!Objects.equals(left.getStatsByStage(), right.getStatsByStage())) {
          if (_failOnFalse) {
            throw new IllegalArgumentException("Different statsByStage: " + left.getStatsByStage() + " and "
                + right.getStatsByStage());
          }
          return false;
        }
      } else {
        if (rightType == DataBlock.Type.METADATA) {
          if (_failOnFalse) {
            throw new IllegalArgumentException("Different types: " + leftType + " and " + rightType);
          }
          return false;
        }

        DataSchema dataSchema = left.getDataSchema();
        assert dataSchema != null;
        if (!dataSchema.equals(right.getDataSchema())) {
          if (_failOnFalse) {
            throw new IllegalArgumentException("Different data schemas: " + dataSchema + " and "
                + right.getDataSchema());
          }
          return false;
        }

        if (left.getNumberOfRows() != right.getNumberOfRows()) {
          if (_failOnFalse) {
            throw new IllegalArgumentException("Different number of rows: " + left.getNumberOfRows() + " and "
                + right.getNumberOfRows());
          }
          return false;
        }
        int numRows = left.getNumberOfRows();
        if (left.getNumberOfColumns() != right.getNumberOfColumns()) {
          if (_failOnFalse) {
            throw new IllegalArgumentException("Different number of columns: " + left.getNumberOfColumns() + " and "
                + right.getNumberOfColumns());
          }
          return false;
        }

        DataSchema.ColumnDataType[] colTypes = dataSchema.getColumnDataTypes();
        String[] colNames = dataSchema.getColumnNames();
        for (int colId = 0; colId < colNames.length; colId++) {
          RoaringBitmap leftNulls = left.getNullRowIds(colId);
          RoaringBitmap rightNulls = right.getNullRowIds(colId);
          if (!Objects.equals(leftNulls, rightNulls)) {
            if (_failOnFalse) {
              throw new IllegalArgumentException("Different nulls for column: " + colNames[colId]
                  + " left: " + leftNulls + " right: " + rightNulls);
            }
            return false;
          }

          switch (colTypes[colId]) {
            case INT:
            case BOOLEAN:
              for (int did = 0; did < numRows; did++) {
                if (left.getInt(did, colId) != right.getInt(did, colId)) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + left.getInt(did, colId) + " right: " + right.getInt(did, colId));
                  }
                  return false;
                }
              }
              break;
            case LONG:
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
              for (int did = 0; did < numRows; did++) {
                if (left.getLong(did, colId) != right.getLong(did, colId)) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + left.getLong(did, colId) + " right: " + right.getLong(did, colId));
                  }
                  return false;
                }
              }
              break;
            case FLOAT:
              for (int did = 0; did < numRows; did++) {
                if (left.getFloat(did, colId) != right.getFloat(did, colId)) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + left.getFloat(did, colId) + " right: " + right.getFloat(did, colId));
                  }
                  return false;
                }
              }
              break;
            case DOUBLE:
              for (int did = 0; did < numRows; did++) {
                if (left.getDouble(did, colId) != right.getDouble(did, colId)) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + left.getDouble(did, colId) + " right: " + right.getDouble(did, colId));
                  }
                  return false;
                }
              }
              break;
            case STRING:
            case JSON:
              for (int did = 0; did < numRows; did++) {
                if (!left.getString(did, colId).equals(right.getString(did, colId))) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + left.getString(did, colId) + " right: " + right.getString(did, colId));
                  }
                  return false;
                }
              }
              break;
            case BYTES:
              for (int did = 0; did < numRows; did++) {
                if (!left.getBytes(did, colId).equals(right.getBytes(did, colId))) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + left.getBytes(did, colId) + " right: " + right.getBytes(did, colId));
                  }
                  return false;
                }
              }
              break;
            case BIG_DECIMAL:
              for (int did = 0; did < numRows; did++) {
                if (!left.getBigDecimal(did, colId).equals(right.getBigDecimal(did, colId))) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + left.getBigDecimal(did, colId) + " right: " + right.getBigDecimal(did, colId));
                  }
                  return false;
                }
              }
              break;
            case OBJECT:
              for (int did = 0; did < numRows; did++) {
                if (!Objects.equals(left.getCustomObject(did, colId), right.getCustomObject(did, colId))) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + left.getCustomObject(did, colId)
                        + " right: " + right.getCustomObject(did, colId));
                  }
                  return false;
                }
              }
              break;
            case INT_ARRAY:
              for (int did = 0; did < numRows; did++) {
                if (!Arrays.equals(left.getIntArray(did, colId), right.getIntArray(did, colId))) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + Arrays.toString(left.getIntArray(did, colId))
                        + " right: " + Arrays.toString(right.getIntArray(did, colId)));
                  }
                  return false;
                }
              }
              break;
            case LONG_ARRAY:
            case TIMESTAMP_ARRAY:
            case TIMESTAMP_NTZ_ARRAY:
              for (int did = 0; did < numRows; did++) {
                if (!Arrays.equals(left.getLongArray(did, colId), right.getLongArray(did, colId))) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + Arrays.toString(left.getLongArray(did, colId))
                        + " right: " + Arrays.toString(right.getLongArray(did, colId)));
                  }
                  return false;
                }
              }
              break;
            case FLOAT_ARRAY:
              for (int did = 0; did < numRows; did++) {
                if (!Arrays.equals(left.getFloatArray(did, colId), right.getFloatArray(did, colId))) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + Arrays.toString(left.getFloatArray(did, colId))
                        + " right: " + Arrays.toString(right.getFloatArray(did, colId)));
                  }
                  return false;
                }
              }
              break;
            case DOUBLE_ARRAY:
              for (int did = 0; did < numRows; did++) {
                if (!Arrays.equals(left.getDoubleArray(did, colId), right.getDoubleArray(did, colId))) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + Arrays.toString(left.getDoubleArray(did, colId))
                        + " right: " + Arrays.toString(right.getDoubleArray(did, colId)));
                  }
                  return false;
                }
              }
              break;
            case STRING_ARRAY:
              for (int did = 0; did < numRows; did++) {
                if (!Arrays.equals(left.getStringArray(did, colId), right.getStringArray(did, colId))) {
                  if (_failOnFalse) {
                    throw new IllegalArgumentException("Different values for " + colTypes[colId]
                        + " column: " + colNames[colId]
                        + " left: " + Arrays.toString(left.getStringArray(did, colId))
                        + " right: " + Arrays.toString(right.getStringArray(did, colId)));
                  }
                  return false;
                }
              }
              break;
            case BYTES_ARRAY:
            case BOOLEAN_ARRAY:
            case UNKNOWN:
              throw new UnsupportedOperationException("Check how to read " + colTypes[colId] + " from data block");
            default:
              throw new UnsupportedOperationException("Unsupported column type: " + colTypes[colId]);
          }
        }
      }
      return true;
    }
  }
}
