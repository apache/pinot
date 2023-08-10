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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BitmapDocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.ProjectionOperatorUtils;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * The {@code ExpressionScanDocIdIterator} is the scan-based iterator for ExpressionFilterDocIdSet that can handle
 * filters on the expressions. It leverages the projection operator to batch processing the records block by block.
 */
public final class ExpressionScanDocIdIterator implements ScanBasedDocIdIterator {
  private final TransformFunction _transformFunction;
  private final PredicateEvaluator _predicateEvaluator;
  private final Map<String, DataSource> _dataSourceMap;
  private final int _endDocId;
  private final int[] _docIdBuffer = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
  private final boolean _nullHandlingEnabled;
  private final PredicateEvaluationResult _predicateEvaluationResult;

  private int _blockEndDocId = 0;
  private PeekableIntIterator _docIdIterator;

  // NOTE: Number of entries scanned is not accurate because we might need to scan multiple columns in order to solve
  //       the expression, but we only track the number of entries scanned for the resolved expression.
  private long _numEntriesScanned = 0L;

  public ExpressionScanDocIdIterator(TransformFunction transformFunction,
      @Nullable PredicateEvaluator predicateEvaluator, Map<String, DataSource> dataSourceMap, int numDocs,
      boolean nullHandlingEnabled, PredicateEvaluationResult predicateEvaluationResult) {
    _transformFunction = transformFunction;
    _predicateEvaluator = predicateEvaluator;
    _dataSourceMap = dataSourceMap;
    _endDocId = numDocs;
    _nullHandlingEnabled = nullHandlingEnabled;
    _predicateEvaluationResult = predicateEvaluationResult;
  }

  @Override
  public int next() {
    // If there are remaining records in the current block, return them first
    if (_docIdIterator != null && _docIdIterator.hasNext()) {
      return _docIdIterator.next();
    }

    // Evaluate the records in the next block
    while (_blockEndDocId < _endDocId) {
      int blockStartDocId = _blockEndDocId;
      _blockEndDocId = Math.min(blockStartDocId + DocIdSetPlanNode.MAX_DOC_PER_CALL, _endDocId);
      ProjectionBlock projectionBlock = ProjectionOperatorUtils.getProjectionOperator(_dataSourceMap,
          new RangeDocIdSetOperator(blockStartDocId, _blockEndDocId)).nextBlock();
      RoaringBitmap matchingDocIds = new RoaringBitmap();
      processProjectionBlock(projectionBlock, matchingDocIds);
      if (!matchingDocIds.isEmpty()) {
        _docIdIterator = matchingDocIds.getIntIterator();
        return _docIdIterator.next();
      }
    }

    return Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId < _blockEndDocId) {
      // Search the current block first
      _docIdIterator.advanceIfNeeded(targetDocId);
      if (_docIdIterator.hasNext()) {
        return _docIdIterator.next();
      }
    } else {
      // Skip the blocks before the target document id
      _blockEndDocId = targetDocId;
    }

    // Search the block following the target document id
    _docIdIterator = null;
    return next();
  }

  @Override
  public MutableRoaringBitmap applyAnd(BatchIterator batchIterator, OptionalInt firstDoc, OptionalInt lastDoc) {
    IntIterator intIterator = batchIterator.asIntIterator(new int[OPTIMAL_ITERATOR_BATCH_SIZE]);
    ProjectionOperator projectionOperator = ProjectionOperatorUtils.getProjectionOperator(_dataSourceMap,
        new BitmapDocIdSetOperator(intIterator, _docIdBuffer));
    MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
    ProjectionBlock projectionBlock;
    while ((projectionBlock = projectionOperator.nextBlock()) != null) {
      processProjectionBlock(projectionBlock, matchingDocIds);
    }
    return matchingDocIds;
  }

  @Override
  public MutableRoaringBitmap applyAnd(ImmutableRoaringBitmap docIds) {
    ProjectionOperator projectionOperator =
        ProjectionOperatorUtils.getProjectionOperator(_dataSourceMap, new BitmapDocIdSetOperator(docIds, _docIdBuffer));
    MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
    ProjectionBlock projectionBlock;
    while ((projectionBlock = projectionOperator.nextBlock()) != null) {
      processProjectionBlock(projectionBlock, matchingDocIds);
    }
    return matchingDocIds;
  }

  private void processProjectionBlock(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds) {
    int numDocs = projectionBlock.getNumDocs();
    TransformResultMetadata resultMetadata = _transformFunction.getResultMetadata();
    if (resultMetadata.isSingleValue()) {
      _numEntriesScanned += numDocs;
      RoaringBitmap nullBitmap = null;
      if (_predicateEvaluationResult == PredicateEvaluationResult.NULL) {
        nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
        if (nullBitmap != null) {
          for (int i : nullBitmap) {
            matchingDocIds.add(_docIdBuffer[i]);
          }
        }
        return;
      }
      boolean predicateEvaluationResult = _predicateEvaluationResult == PredicateEvaluationResult.TRUE;
      assert (_predicateEvaluator != null);
      if (resultMetadata.hasDictionary()) {
        int[] dictIds = _transformFunction.transformToDictIdsSV(projectionBlock);
        if (_nullHandlingEnabled) {
          nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < numDocs; i++) {
            if (_predicateEvaluator.applySV(dictIds[i]) == predicateEvaluationResult && !nullBitmap.contains(i)) {
              matchingDocIds.add(_docIdBuffer[i]);
            }
          }
        } else {
          for (int i = 0; i < numDocs; i++) {
            if (_predicateEvaluator.applySV(dictIds[i]) == predicateEvaluationResult) {
              matchingDocIds.add(_docIdBuffer[i]);
            }
          }
        }
      } else {
        switch (resultMetadata.getDataType().getStoredType()) {
          case INT:
            int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
            if (_nullHandlingEnabled) {
              nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
            }
            if (nullBitmap != null && !nullBitmap.isEmpty()) {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(intValues[i]) == predicateEvaluationResult && !nullBitmap.contains(i)) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            } else {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(intValues[i]) == predicateEvaluationResult) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            }
            break;
          case LONG:
            long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
            if (_nullHandlingEnabled) {
              nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
            }
            if (nullBitmap != null && !nullBitmap.isEmpty()) {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(longValues[i]) == predicateEvaluationResult && !nullBitmap.contains(
                    i)) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            } else {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(longValues[i]) == predicateEvaluationResult) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            }
            break;
          case FLOAT:
            float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
            if (_nullHandlingEnabled) {
              nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
            }
            if (nullBitmap != null && !nullBitmap.isEmpty()) {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(floatValues[i]) == predicateEvaluationResult && !nullBitmap.contains(
                    i)) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            } else {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(floatValues[i]) == predicateEvaluationResult) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            }
            break;
          case DOUBLE:
            double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
            if (_nullHandlingEnabled) {
              nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
            }
            if (nullBitmap != null && !nullBitmap.isEmpty()) {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(doubleValues[i]) == predicateEvaluationResult && !nullBitmap.contains(
                    i)) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            } else {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(doubleValues[i]) == predicateEvaluationResult) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            }
            break;
          case STRING:
            String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
            if (_nullHandlingEnabled) {
              nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
            }
            if (nullBitmap != null && !nullBitmap.isEmpty()) {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(stringValues[i]) == predicateEvaluationResult && !nullBitmap.contains(
                    i)) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            } else {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(stringValues[i]) == predicateEvaluationResult) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            }
            break;
          case BYTES:
            byte[][] bytesValues = _transformFunction.transformToBytesValuesSV(projectionBlock);
            if (_nullHandlingEnabled) {
              nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
            }
            if (nullBitmap != null && !nullBitmap.isEmpty()) {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(bytesValues[i]) == predicateEvaluationResult && !nullBitmap.contains(
                    i)) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            } else {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(bytesValues[i]) == predicateEvaluationResult) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            }
            break;
          case BIG_DECIMAL:
            BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
            if (_nullHandlingEnabled) {
              nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
            }
            if (nullBitmap != null && !nullBitmap.isEmpty()) {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(bigDecimalValues[i]) == predicateEvaluationResult
                    && !nullBitmap.contains(i)) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            } else {
              for (int i = 0; i < numDocs; i++) {
                if (_predicateEvaluator.applySV(bigDecimalValues[i]) == predicateEvaluationResult) {
                  matchingDocIds.add(_docIdBuffer[i]);
                }
              }
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    } else {
      // TODO(https://github.com/apache/pinot/issues/10882): support NULL for multi-value.
      if (_predicateEvaluationResult == PredicateEvaluationResult.NULL) {
        return;
      }
      boolean predicateEvaluationResult = _predicateEvaluationResult == PredicateEvaluationResult.TRUE;
      assert (_predicateEvaluator != null);
      if (resultMetadata.hasDictionary()) {
        int[][] dictIdsArray = _transformFunction.transformToDictIdsMV(projectionBlock);
        for (int i = 0; i < numDocs; i++) {
          int[] dictIds = dictIdsArray[i];
          int numDictIds = dictIds.length;
          _numEntriesScanned += numDictIds;
          if (_predicateEvaluator.applyMV(dictIds, numDictIds) == predicateEvaluationResult) {
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
              if (_predicateEvaluator.applyMV(values, numValues) == predicateEvaluationResult) {
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
              if (_predicateEvaluator.applyMV(values, numValues) == predicateEvaluationResult) {
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
              if (_predicateEvaluator.applyMV(values, numValues) == predicateEvaluationResult) {
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
              if (_predicateEvaluator.applyMV(values, numValues) == predicateEvaluationResult) {
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
              if (_predicateEvaluator.applyMV(values, numValues) == predicateEvaluationResult) {
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

  @Override
  public long getNumEntriesScanned() {
    return _numEntriesScanned;
  }

  /**
   * NOTE: This operator contains only one block.
   */
  private class RangeDocIdSetOperator extends BaseOperator<DocIdSetBlock> {
    static final String EXPLAIN_NAME = "DOC_ID_SET_RANGE";

    DocIdSetBlock _docIdSetBlock;

    RangeDocIdSetOperator(int startDocId, int endDocId) {
      int numDocs = endDocId - startDocId;
      for (int i = 0; i < numDocs; i++) {
        _docIdBuffer[i] = startDocId + i;
      }
      _docIdSetBlock = new DocIdSetBlock(_docIdBuffer, numDocs);
    }

    @Override
    protected DocIdSetBlock getNextBlock() {
      DocIdSetBlock docIdSetBlock = _docIdSetBlock;
      _docIdSetBlock = null;
      return docIdSetBlock;
    }

    @Override
    public String toExplainString() {
      return EXPLAIN_NAME;
    }

    @Override
    public List<Operator> getChildOperators() {
      return Collections.emptyList();
    }
  }

  public enum PredicateEvaluationResult {
    TRUE, NULL, FALSE
  }
}
