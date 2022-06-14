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
package org.apache.pinot.core.query.distinct;

public class DistinctExecutorUtils {
  private DistinctExecutorUtils() {
  }

  /**
   * Returns an array of dictionary ids for a given document on multiple expressions (SV or MV).
   */
  public static int[][] getDictIds(int[][] svDictIds, int[][][] mvDictIds, int docId) {
    int[][] dictIdsArray = null;

    // Before converting to array, keep single dict ids for better performance
    int numExpressions = svDictIds.length;
    int[] singleDictIds = new int[numExpressions];

    for (int i = 0; i < numExpressions; i++) {
      if (svDictIds[i] != null) {
        int dictId = svDictIds[i][docId];
        if (dictIdsArray == null) {
          singleDictIds[i] = dictId;
        } else {
          for (int[] dictIds : dictIdsArray) {
            dictIds[i] = dictId;
          }
        }
      } else {
        int[] dictIdsForMV = mvDictIds[i][docId];
        int numValues = dictIdsForMV.length;

        // Specialize multi-value column with only one value inside
        if (numValues == 1) {
          int dictId = dictIdsForMV[0];
          if (dictIdsArray == null) {
            singleDictIds[i] = dictId;
          } else {
            for (int[] dictIds : dictIdsArray) {
              dictIds[i] = dictId;
            }
          }
        } else {
          if (dictIdsArray == null) {
            dictIdsArray = new int[numValues][];
            for (int j = 0; j < numValues; j++) {
              int dictId = dictIdsForMV[j];
              dictIdsArray[j] = singleDictIds.clone();
              dictIdsArray[j][i] = dictId;
            }
          } else {
            int currentLength = dictIdsArray.length;
            int newLength = currentLength * numValues;
            int[][] newDictIdsArray = new int[newLength][];
            System.arraycopy(dictIdsArray, 0, newDictIdsArray, 0, currentLength);
            for (int j = 1; j < numValues; j++) {
              int offset = j * currentLength;
              for (int k = 0; k < currentLength; k++) {
                newDictIdsArray[offset + k] = dictIdsArray[k].clone();
              }
            }
            for (int j = 0; j < numValues; j++) {
              int dictId = dictIdsForMV[j];
              int startOffset = j * currentLength;
              int endOffset = startOffset + currentLength;
              for (int k = startOffset; k < endOffset; k++) {
                newDictIdsArray[k][i] = dictId;
              }
            }
            dictIdsArray = newDictIdsArray;
          }
        }
      }
    }

    return dictIdsArray == null ? new int[][]{singleDictIds} : dictIdsArray;
  }

  /**
   * Returns an array of records for a given document on multiple expressions (SV or MV).
   */
  public static Object[][] getRecords(Object[][] svValues, Object[][][] mvValues, int docId) {
    Object[][] records = null;

    // Before converting to array, keep single record for better performance
    int numExpressions = svValues.length;
    Object[] singleRecord = new Object[numExpressions];

    for (int i = 0; i < numExpressions; i++) {
      if (svValues[i] != null) {
        Object value = svValues[i][docId];
        if (records == null) {
          singleRecord[i] = value;
        } else {
          for (Object[] record : records) {
            record[i] = value;
          }
        }
      } else {
        Object[] valuesForMV = mvValues[i][docId];
        int numValues = valuesForMV.length;

        // Specialize multi-value column with only one value inside
        if (numValues == 1) {
          Object value = valuesForMV[0];
          if (records == null) {
            singleRecord[i] = value;
          } else {
            for (Object[] record : records) {
              record[i] = value;
            }
          }
        } else {
          if (records == null) {
            records = new Object[numValues][];
            for (int j = 0; j < numValues; j++) {
              Object value = valuesForMV[j];
              records[j] = singleRecord.clone();
              records[j][i] = value;
            }
          } else {
            int currentLength = records.length;
            int newLength = currentLength * numValues;
            Object[][] newRecords = new Object[newLength][];
            System.arraycopy(records, 0, newRecords, 0, currentLength);
            for (int j = 1; j < numValues; j++) {
              int offset = j * currentLength;
              for (int k = 0; k < currentLength; k++) {
                newRecords[offset + k] = records[k].clone();
              }
            }
            for (int j = 0; j < numValues; j++) {
              Object value = valuesForMV[j];
              int startOffset = j * currentLength;
              int endOffset = startOffset + currentLength;
              for (int k = startOffset; k < endOffset; k++) {
                newRecords[k][i] = value;
              }
            }
            records = newRecords;
          }
        }
      }
    }

    return records == null ? new Object[][]{singleRecord} : records;
  }
}
