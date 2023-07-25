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
package org.apache.pinot.core.operator.dociditerators;

import java.math.BigDecimal;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.BitmapDataProvider;


/**
 * The {@code ExpressionEvaluatesToTrueScanDocIdIterator} returns the document IDs that the predicate evaluates to true.
 */
public class ExpressionEvaluatesToTrueWithoutNullHandlingScanDocIdIterator extends ExpressionScanDocIdIterator {
  private final PredicateEvaluator _predicateEvaluator;

  public ExpressionEvaluatesToTrueWithoutNullHandlingScanDocIdIterator(TransformFunction transformFunction,
      PredicateEvaluator predicateEvaluator, Map<String, DataSource> dataSourceMap, int numDocs) {
    super(transformFunction, dataSourceMap, numDocs);
    _predicateEvaluator = predicateEvaluator;
  }

  @Override
  protected void processProjectionBlock(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds) {
    int numDocs = projectionBlock.getNumDocs();
    TransformResultMetadata resultMetadata = _transformFunction.getResultMetadata();
    if (resultMetadata.isSingleValue()) {
      _numEntriesScanned += numDocs;
      if (resultMetadata.hasDictionary()) {
        int[] dictIds = _transformFunction.transformToDictIdsSV(projectionBlock);
        for (int i = 0; i < numDocs; i++) {
          if (_predicateEvaluator.applySV(dictIds[i])) {
            matchingDocIds.add(_docIdBuffer[i]);
          }
        }
      } else {
        switch (resultMetadata.getDataType().getStoredType()) {
          case INT:
            int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              if (_predicateEvaluator.applySV(intValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case LONG:
            long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              if (_predicateEvaluator.applySV(longValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case FLOAT:
            float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              if (_predicateEvaluator.applySV(floatValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case DOUBLE:
            double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              if (_predicateEvaluator.applySV(doubleValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case STRING:
            String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              if (_predicateEvaluator.applySV(stringValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case BYTES:
            byte[][] bytesValues = _transformFunction.transformToBytesValuesSV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              if (_predicateEvaluator.applySV(bytesValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case BIG_DECIMAL:
            BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              if (_predicateEvaluator.applySV(bigDecimalValues[i])) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    } else {
      if (resultMetadata.hasDictionary()) {
        int[][] dictIdsArray = _transformFunction.transformToDictIdsMV(projectionBlock);
        for (int i = 0; i < numDocs; i++) {
          int[] dictIds = dictIdsArray[i];
          int numDictIds = dictIds.length;
          _numEntriesScanned += numDictIds;
          if (_predicateEvaluator.applyMV(dictIds, numDictIds)) {
            matchingDocIds.add(_docIdBuffer[i]);
          }
        }
      } else {
        switch (resultMetadata.getDataType().getStoredType()) {
          case INT:
            int[][] intValuesArray = _transformFunction.transformToIntValuesMV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              int[] values = intValuesArray[i];
              int numValues = values.length;
              _numEntriesScanned += numValues;
              if (_predicateEvaluator.applyMV(values, numValues)) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case LONG:
            long[][] longValuesArray = _transformFunction.transformToLongValuesMV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              long[] values = longValuesArray[i];
              int numValues = values.length;
              _numEntriesScanned += numValues;
              if (_predicateEvaluator.applyMV(values, numValues)) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case FLOAT:
            float[][] floatValuesArray = _transformFunction.transformToFloatValuesMV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              float[] values = floatValuesArray[i];
              int numValues = values.length;
              _numEntriesScanned += numValues;
              if (_predicateEvaluator.applyMV(values, numValues)) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case DOUBLE:
            double[][] doubleValuesArray = _transformFunction.transformToDoubleValuesMV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              double[] values = doubleValuesArray[i];
              int numValues = values.length;
              _numEntriesScanned += numValues;
              if (_predicateEvaluator.applyMV(values, numValues)) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          case STRING:
            String[][] valuesArray = _transformFunction.transformToStringValuesMV(projectionBlock);
            for (int i = 0; i < numDocs; i++) {
              String[] values = valuesArray[i];
              int numValues = values.length;
              _numEntriesScanned += numValues;
              if (_predicateEvaluator.applyMV(values, numValues)) {
                matchingDocIds.add(_docIdBuffer[i]);
              }
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }
  }
}
