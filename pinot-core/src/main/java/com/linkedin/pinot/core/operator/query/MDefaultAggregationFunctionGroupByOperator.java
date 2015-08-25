/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.query;

import java.io.Serializable;
import java.util.ArrayList;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.DocIdSetBlock;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


/**
 * The most generic GroupBy Operator which will take all the required
 * dataSources and do aggregation and groupBy.
 *
 *
 */
public class MDefaultAggregationFunctionGroupByOperator extends AggregationFunctionGroupByOperator {

  private final BlockValIterator[] _groupByBlockValIterators;

  public MDefaultAggregationFunctionGroupByOperator(AggregationInfo aggregationInfo, GroupBy groupBy,
      Operator projectionOperator, boolean hasDictionary) {
    super(aggregationInfo, groupBy, projectionOperator, hasDictionary);
    _groupByBlockValIterators = new BlockValIterator[_groupBy.getColumnsSize()];

  }

  @Override
  public Block getNextBlock() {
    ProjectionBlock block = (ProjectionBlock) _projectionOperator.nextBlock();
    if (block == null) {
      return null;
    }

    DocIdSetBlock docIdSetBlock = (DocIdSetBlock) block.getDocIdSetBlock();
    BlockDocIdIterator blockDocIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    int docId = 0;

    for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
      _groupByBlockValIterators[i] = block.getBlock(_groupBy.getColumns().get(i)).getBlockValueSet().iterator();
    }

    while ((docId = blockDocIdIterator.next()) != Constants.EOF) {
      if (!_isGroupByColumnsContainMultiValueColumn) {
        String groupKey = getGroupKey(docId);
        Serializable aggregate =
            _aggregationFunction.aggregate(_aggregateGroupedValue.get(groupKey), docId, _aggregationFunctionBlocks);
        _aggregateGroupedValue.put(groupKey, aggregate);
      } else {
        String[] groupKeys = getGroupKeys(docId);
        for (String groupKey : groupKeys) {
          Serializable aggregate =
              _aggregationFunction.aggregate(_aggregateGroupedValue.get(groupKey), docId, _aggregationFunctionBlocks);
          _aggregateGroupedValue.put(groupKey, aggregate);
        }
      }
    }
    return null;
  }

  private String getGroupKey(int docId) {
    String groupKey = "";
    BlockSingleValIterator blockValIterator;

    for (int i = 0; i < _groupByBlockValIterators.length; ++i) {
      if (i > 0) {
        groupKey += GroupByConstants.GroupByDelimiter.groupByMultiDelimeter;
      }
      blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[i];
      blockValIterator.skipTo(docId);

      if (_groupByBlocks[i].getMetadata().hasDictionary()) {
        int nextIntVal = blockValIterator.nextIntVal();
        groupKey += ((_groupByBlocks[i].getMetadata().getDictionary().get(nextIntVal).toString()));
      } else {
        switch (_groupByBlocks[i].getMetadata().getDataType()) {
          case INT:
            groupKey += blockValIterator.nextIntVal();
            break;
          case FLOAT:
            groupKey += blockValIterator.nextFloatVal();
            break;
          case LONG:
            groupKey += blockValIterator.nextLongVal();
            break;
          case DOUBLE:
            groupKey += blockValIterator.nextDoubleVal();
            break;

          default:
            break;
        }
      }
    }
    return groupKey;
  }

  private String[] getGroupKeys(int docId) {
    ArrayList<String> groupKeyList = new ArrayList<String>();
    Dictionary dictionaryReader = _groupByBlocks[0].getMetadata().getDictionary();
    if (_groupByBlocks[0].getMetadata().isSingleValue()) {
      BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[0];
      blockValIterator.skipTo(docId);
      if (dictionaryReader != null) {
        groupKeyList.add(dictionaryReader.get(blockValIterator.nextIntVal()).toString());
      } else {
        switch (_groupByBlocks[0].getMetadata().getDataType()) {
          case INT:
            groupKeyList.add(blockValIterator.nextIntVal() + "");
            break;
          case FLOAT:
            groupKeyList.add(blockValIterator.nextFloatVal() + "");
            break;
          case LONG:
            groupKeyList.add(blockValIterator.nextLongVal() + "");
            break;
          case DOUBLE:
            groupKeyList.add(blockValIterator.nextDoubleVal() + "");
            break;
          default:
            break;
        }
      }
    } else {
      BlockMultiValIterator blockValIterator = (BlockMultiValIterator) _groupByBlockValIterators[0];
      blockValIterator.skipTo(docId);
      final int maxValue = _groupByBlocks[0].getMetadata().getMaxNumberOfMultiValues();

      if (dictionaryReader != null) {
        final int[] entries = new int[maxValue];
        int groups = blockValIterator.nextIntVal(entries);
        for (int i = 0; i < groups; ++i) {
          groupKeyList.add((dictionaryReader.get(entries[i])).toString());
        }
      } else {
        switch (_groupByBlocks[0].getMetadata().getDataType()) {
          case INT:
            final int[] intEntries = new int[maxValue];
            int intGroups = blockValIterator.nextIntVal(intEntries);
            for (int i = 0; i < intGroups; ++i) {
              groupKeyList.add(intEntries[i] + "");
            }
            break;
          case FLOAT:
            final float[] floatEntries = new float[maxValue];
            int floatGroups = blockValIterator.nextFloatVal(floatEntries);
            for (int i = 0; i < floatGroups; ++i) {
              groupKeyList.add(floatEntries[i] + "");
            }
            break;
          case LONG:
            final long[] longEntries = new long[maxValue];
            int longGroups = blockValIterator.nextLongVal(longEntries);
            for (int i = 0; i < longGroups; ++i) {
              groupKeyList.add(longEntries[i] + "");
            }
            break;
          case DOUBLE:
            final double[] doubleEntries = new double[maxValue];
            int doubleGroups = blockValIterator.nextDoubleVal(doubleEntries);
            for (int i = 0; i < doubleGroups; ++i) {
              groupKeyList.add(doubleEntries[i] + "");
            }
            break;
          default:
            break;
        }
      }
    }

    for (int i = 1; i < _groupByBlockValIterators.length; ++i) {
      dictionaryReader = _groupByBlocks[i].getMetadata().getDictionary();
      if (_groupByBlocks[i].getMetadata().isSingleValue()) {
        BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[i];
        blockValIterator.skipTo(docId);
        if (dictionaryReader != null) {
          for (int j = 0; j < groupKeyList.size(); ++j) {
            groupKeyList.set(
                j,
                groupKeyList.get(j)
                    + (GroupByConstants.GroupByDelimiter.groupByMultiDelimeter + (dictionaryReader.get(blockValIterator
                        .nextIntVal()).toString())));
          }
        } else {
          switch (_groupByBlocks[i].getMetadata().getDataType()) {
            case INT:
              for (int j = 0; j < groupKeyList.size(); ++j) {
                groupKeyList.set(j, groupKeyList.get(j) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
                    + blockValIterator.nextIntVal());
              }
              break;
            case FLOAT:
              for (int j = 0; j < groupKeyList.size(); ++j) {
                groupKeyList.set(j, groupKeyList.get(j) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
                    + blockValIterator.nextFloatVal());
              }
              break;
            case LONG:
              for (int j = 0; j < groupKeyList.size(); ++j) {
                groupKeyList.set(j, groupKeyList.get(j) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
                    + blockValIterator.nextLongVal());
              }
              break;
            case DOUBLE:
              for (int j = 0; j < groupKeyList.size(); ++j) {
                groupKeyList.set(j, groupKeyList.get(j) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
                    + blockValIterator.nextDoubleVal());
              }
              break;
            default:
              break;
          }
        }
      } else {
        // Multivalue
        BlockMultiValIterator blockValIterator = (BlockMultiValIterator) _groupByBlockValIterators[i];
        blockValIterator.skipTo(docId);
        final int maxValue = _groupByBlocks[i].getMetadata().getMaxNumberOfMultiValues();
        int currentGroupListSize = groupKeyList.size();
        if (dictionaryReader != null) {
          final int[] entries = new int[maxValue];
          int groups = blockValIterator.nextIntVal(entries);
          for (int j = 1; j < groups; ++j) {
            for (int k = 0; k < currentGroupListSize; ++k) {
              groupKeyList.add(groupKeyList.get(k));
            }
          }

          for (int j = 0; j < groupKeyList.size(); ++j) {
            groupKeyList.set(j, groupKeyList.get(j) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
                + (dictionaryReader.get(entries[j / currentGroupListSize])).toString());
          }
        } else {
          switch (_groupByBlocks[0].getMetadata().getDataType()) {
            case INT:
              final int[] intEntries = new int[maxValue];
              int intGroups = blockValIterator.nextIntVal(intEntries);
              for (int j = 1; j < intGroups; ++j) {
                for (int k = 0; k < currentGroupListSize; ++k) {
                  groupKeyList.add(groupKeyList.get(k));
                }
              }
              for (int j = 0; j < groupKeyList.size(); ++j) {
                groupKeyList.set(j, groupKeyList.get(j) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
                    + intEntries[j / currentGroupListSize]);
              }
              break;
            case FLOAT:
              final float[] floatEntries = new float[maxValue];
              int floatGroups = blockValIterator.nextFloatVal(floatEntries);
              for (int j = 1; j < floatGroups; ++j) {
                for (int k = 0; k < currentGroupListSize; ++k) {
                  groupKeyList.add(groupKeyList.get(k));
                }
              }
              for (int j = 0; j < groupKeyList.size(); ++j) {
                groupKeyList.set(j, groupKeyList.get(j) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
                    + floatEntries[j / currentGroupListSize]);
              }
              break;
            case LONG:
              final long[] longEntries = new long[maxValue];
              int longGroups = blockValIterator.nextLongVal(longEntries);
              for (int j = 1; j < longGroups; ++j) {
                for (int k = 0; k < currentGroupListSize; ++k) {
                  groupKeyList.add(groupKeyList.get(k));
                }
              }
              for (int j = 0; j < groupKeyList.size(); ++j) {
                groupKeyList.set(j, groupKeyList.get(j) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
                    + longEntries[j / currentGroupListSize]);
              }
              break;
            case DOUBLE:
              final double[] doubleEntries = new double[maxValue];
              int doubleGroups = blockValIterator.nextDoubleVal(doubleEntries);
              for (int j = 1; j < doubleGroups; ++j) {
                for (int k = 0; k < currentGroupListSize; ++k) {
                  groupKeyList.add(groupKeyList.get(k));
                }
              }
              for (int j = 0; j < groupKeyList.size(); ++j) {
                groupKeyList.set(j, groupKeyList.get(j) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
                    + doubleEntries[j / currentGroupListSize]);
              }
              break;
            default:
              break;
          }
        }

      }
    }
    return groupKeyList.toArray(new String[0]);
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException(
        "Method: nextBlock(BlockId BlockId) is Not Supported in MAggregationFunctionGroupByOperator");
  }

  @Override
  public String getOperatorName() {
    return "MDefaultAggregationFunctionGroupByOperator";
  }

}
