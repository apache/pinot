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
package org.apache.pinot.core.operator.filter;

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Constants;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.common.DataFetcher;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.dociditerators.RangelessBitmapDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class ExpressionFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "ExpressionFilterOperator";
  private final int _numDocs;
  private TransformFunction _transformFunction;
  private final PredicateEvaluator _predicateEvaluator;
  private final TransformResultMetadata _resultMetadata;
  private final Map<String, DataSource> _dataSourceMap;
  private TransformExpressionTree _expression;

  public ExpressionFilterOperator(IndexSegment segment, TransformExpressionTree expression, Predicate predicate) {
    _expression = expression;
    Set<String> columns = new HashSet<>();
    expression.getColumns(columns);
    _dataSourceMap = new HashMap<>();
    for (String column : columns) {
      _dataSourceMap.put(column, segment.getDataSource(column));
    }
    _transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    _resultMetadata = _transformFunction.getResultMetadata();

    Dictionary dictionary = null;
    if (_resultMetadata.hasDictionary()) {
      dictionary = _transformFunction.getDictionary();
    }
    _predicateEvaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dictionary, _resultMetadata.getDataType());
    _numDocs = segment.getSegmentMetadata().getTotalRawDocs();
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(new ExpressionFilterBlockDocIdSet(this));
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  private static class BitmapWrappedFilterOperator extends BaseFilterOperator {

    private MutableRoaringBitmap _bitmap;

    public BitmapWrappedFilterOperator(MutableRoaringBitmap bitmap) {
      _bitmap = bitmap;
    }

    @Override
    protected FilterBlock getNextBlock() {
      return new FilterBlock(new FilterBlockDocIdSet() {
        @Override
        public int getMinDocId() {
          throw new UnsupportedOperationException("This filter block should be used to iterate over a bitmap");
        }

        @Override
        public int getMaxDocId() {
          throw new UnsupportedOperationException("This filter block should be used to iterate over a bitmap");

        }

        @Override
        public void setStartDocId(int startDocId) {
          throw new UnsupportedOperationException("This filter block should be used to iterate over a bitmap");
        }

        @Override
        public void setEndDocId(int endDocId) {
          throw new UnsupportedOperationException("This filter block should be used to iterate over a bitmap");
        }

        @Override
        public long getNumEntriesScannedInFilter() {
          throw new UnsupportedOperationException("This filter block should be used to iterate over a bitmap");
        }

        @Override
        public BlockDocIdIterator iterator() {
          return new RangelessBitmapDocIdIterator(_bitmap.getIntIterator());
        }

        @Override
        public MutableRoaringBitmap getRaw() {
          return _bitmap;
        }
      });
    }

    @Override
    public String getOperatorName() {
      return BitmapWrappedFilterOperator.class.getName();
    }
  }

  private static class ExpressionFilterBlockDocIdSet implements FilterBlockDocIdSet {
    private final ExpressionSVBlockDocIdIterator _blockDocIdIterator;
    private ExpressionFilterOperator _expressionFilterOperator;

    public ExpressionFilterBlockDocIdSet(ExpressionFilterOperator expressionFilterOperator) {
      _expressionFilterOperator = expressionFilterOperator;
      if (expressionFilterOperator._resultMetadata.isSingleValue()) {
        _blockDocIdIterator = new ExpressionSVBlockDocIdIterator(_expressionFilterOperator);
      } else {
        throw new UnsupportedOperationException("Filter on expressions that return multi-values is not yet supported");
      }
      _blockDocIdIterator.setStartDocId(0);
      _blockDocIdIterator.setEndDocId(_expressionFilterOperator._numDocs - 1);
    }

    @Override
    public int getMinDocId() {
      return _blockDocIdIterator._startDocId;
    }

    @Override
    public int getMaxDocId() {
      return _blockDocIdIterator._endDocId;
    }

    @Override
    public void setStartDocId(int startDocId) {
      _blockDocIdIterator.setStartDocId(startDocId);
    }

    @Override
    public void setEndDocId(int endDocId) {
      _blockDocIdIterator.setEndDocId(endDocId);
    }

    @Override
    public long getNumEntriesScannedInFilter() {
      return _blockDocIdIterator.getNumEntriesScanned();
    }

    @Override
    public BlockDocIdIterator iterator() {
      return _blockDocIdIterator;
    }

    @Override
    public <T> T getRaw() {
      throw new UnsupportedOperationException("getRaw not supported for ScanBasedDocIdSet");
    }

    private static class ExpressionSVBlockDocIdIterator implements ScanBasedDocIdIterator {
      private ExpressionFilterOperator _expressionFilterOperator;
      int _numDocsScanned = 0;
      int _startDocId;
      int _endDocId;
      //used only in next() and advance methods
      int _currentBlockStartDocId = -1;
      int _currentBlockEndDocId = 0;
      private int _currentDocId = -1;
      IntIterator _intIterator = null;

      public ExpressionSVBlockDocIdIterator(ExpressionFilterOperator expressionFilterOperator) {
        _expressionFilterOperator = expressionFilterOperator;
      }

      public void setStartDocId(int startDocId) {
        _startDocId = startDocId;
      }

      public void setEndDocId(int endDocId) {
        _endDocId = endDocId;
      }

      @Override
      public int next() {
        if (_currentDocId == Constants.EOF) {
          return Constants.EOF;
        }
        while (_currentDocId < _endDocId) {
          if (_intIterator == null) {
            _currentBlockStartDocId = _currentBlockEndDocId;
            _currentBlockEndDocId = _currentBlockStartDocId + DocIdSetPlanNode.MAX_DOC_PER_CALL;
            _currentBlockEndDocId = Math.min(_currentBlockEndDocId, _endDocId);
            MutableRoaringBitmap bitmapRange = new MutableRoaringBitmap();
            bitmapRange.add(_currentBlockStartDocId, _currentBlockEndDocId + 1);
            MutableRoaringBitmap matchedBitmap = evaluate(bitmapRange);
            _intIterator = matchedBitmap.getIntIterator();
            _numDocsScanned += (_currentBlockEndDocId - _currentBlockStartDocId);
          }
          if (_intIterator.hasNext()) {
            return (_currentDocId = _intIterator.next());
          } else {
            _currentDocId = _currentBlockEndDocId;
            _intIterator = null;
          }
        }
        _currentDocId = Constants.EOF;
        return _currentDocId;
      }

      @Override
      public int advance(int targetDocId) {
        if (_currentDocId == Constants.EOF) {
          return _currentDocId;
        }
        if (targetDocId < _startDocId) {
          targetDocId = _startDocId;
        } else if (targetDocId > _endDocId) {
          _currentDocId = Constants.EOF;
          return _currentDocId;
        }
        if(targetDocId >= _currentBlockStartDocId && targetDocId < _currentBlockEndDocId) {
          if(_currentDocId == targetDocId) {
            return _currentDocId;
          }
          while(_intIterator.hasNext()) {
            _currentDocId = _intIterator.next();
            if(_currentDocId >= targetDocId) {
              return _currentDocId;
            }
          }
        }
        //cannot find targetDocId in the current block. start searching on a new block that starts from targetDocId
        //TODO: Consider lowering the block size for advance scenario for e.g. use 1k blocks instead of 10k
        _currentBlockEndDocId = targetDocId;
        _intIterator = null;
        return next();
      }

      @Override
      public int currentDocId() {
        return 0;
      }

      @Override
      public boolean isMatch(int docId) {
        final MutableRoaringBitmap docIdBitmap = new MutableRoaringBitmap();
        docIdBitmap.add(docId);
        final MutableRoaringBitmap evaluate = evaluate(docIdBitmap);
        return evaluate.getCardinality() > 0;
      }

      @Override
      public MutableRoaringBitmap applyAnd(MutableRoaringBitmap answer) {
        MutableRoaringBitmap bitmap = evaluate(answer);
        answer.and(bitmap);
        return answer;
      }

      private MutableRoaringBitmap evaluate(MutableRoaringBitmap answer) {
        BaseFilterOperator filterOperator = new BitmapWrappedFilterOperator(answer);
        DocIdSetOperator docIdSetOperator = new DocIdSetOperator(filterOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
        ProjectionOperator projectionOperator =
            new ProjectionOperator(_expressionFilterOperator._dataSourceMap, docIdSetOperator);
        TransformOperator operator =
            new TransformOperator(projectionOperator, Lists.newArrayList(_expressionFilterOperator._expression));
        TransformBlock transformBlock;
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        while ((transformBlock = operator.nextBlock()) != null) {
          DocIdSetBlock docIdSetBlock = transformBlock.getDocIdSetBlock();
          int[] docIdSet = docIdSetBlock.getDocIdSet();
          int length = docIdSetBlock.getSearchableLength();
          _numDocsScanned += length;
          BlockValSet blockValueSet = transformBlock.getBlockValueSet(_expressionFilterOperator._expression);
          if(_expressionFilterOperator._resultMetadata.hasDictionary()) {
            int[] dictionaryIdsSV = blockValueSet.getDictionaryIdsSV();
            for (int i = 0; i < length; i++) {
              if (_expressionFilterOperator._predicateEvaluator.applySV(dictionaryIdsSV[i])) {
                bitmap.add(docIdSet[i]);
              }
            }
          } else {
            switch (_expressionFilterOperator._resultMetadata.getDataType()) {
              case INT:
                int[] intValuesSV = blockValueSet.getIntValuesSV();
                for (int i = 0; i < length; i++) {
                  if (_expressionFilterOperator._predicateEvaluator.applySV(intValuesSV[i])) {
                    bitmap.add(docIdSet[i]);
                  }
                }
                break;
              case LONG:
                long[] longValuesSV = blockValueSet.getLongValuesSV();
                for (int i = 0; i < length; i++) {
                  if (_expressionFilterOperator._predicateEvaluator.applySV(longValuesSV[i])) {
                    bitmap.add(docIdSet[i]);
                  }
                }
                break;
              case FLOAT:
                float[] floatValuesSV = blockValueSet.getFloatValuesSV();
                for (int i = 0; i < length; i++) {
                  if (_expressionFilterOperator._predicateEvaluator.applySV(floatValuesSV[i])) {
                    bitmap.add(docIdSet[i]);
                  }
                }
                break;
              case DOUBLE:
                double[] doubleValuesSV = blockValueSet.getDoubleValuesSV();
                for (int i = 0; i < length; i++) {
                  if (_expressionFilterOperator._predicateEvaluator.applySV(doubleValuesSV[i])) {
                    bitmap.add(docIdSet[i]);
                  }
                }
                break;
              case BOOLEAN:
              case STRING:
                String[] stringValuesSV = blockValueSet.getStringValuesSV();
                for (int i = 0; i < length; i++) {
                  if (_expressionFilterOperator._predicateEvaluator.applySV(stringValuesSV[i])) {
                    bitmap.add(docIdSet[i]);
                  }
                }
                break;
              case BYTES:
                throw new UnsupportedOperationException("Applying filter on bytes column is not supported (yet)");
            }
          }
        }
        return bitmap;
      }

      @Override
      public int getNumEntriesScanned() {
        return _numDocsScanned;
      }
    }
  }
}
