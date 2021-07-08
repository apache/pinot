package org.apache.pinot.core.operator.query;

import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntPriorityQueue;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Operator which executes DISTINCT operation based on dictionary
 */
public class DictionaryBasedDistinctOperator extends DistinctOperator {
    private static final String OPERATOR_NAME = "DictionaryBasedDistinctOperator";
    int MAX_INITIAL_CAPACITY = 10000;

    private final DistinctAggregationFunction _distinctAggregationFunction;
    private final Map<String, Dictionary> _dictionaryMap;
    private final int _numTotalDocs;
    private final TransformOperator _transformOperator;
    private final IntOpenHashSet _dictIdSet;

    private IntPriorityQueue _priorityQueue;

    private boolean _hasOrderBy;

    public DictionaryBasedDistinctOperator(IndexSegment indexSegment, DistinctAggregationFunction distinctAggregationFunction,
                                           Map<String, Dictionary> dictionaryMap, int numTotalDocs,
                                           TransformOperator transformOperator) {
        super(indexSegment, distinctAggregationFunction, transformOperator);

        _distinctAggregationFunction = distinctAggregationFunction;
        _dictionaryMap = dictionaryMap;
        _numTotalDocs = numTotalDocs;
        _transformOperator = transformOperator;
        _dictIdSet = new IntOpenHashSet();

        List<OrderByExpressionContext> orderByExpressionContexts = _distinctAggregationFunction.getOrderByExpressions();

        if (orderByExpressionContexts != null) {
            OrderByExpressionContext orderByExpressionContext = orderByExpressionContexts.get(0);
            int limit = _distinctAggregationFunction.getLimit();
            int comparisonFactor = orderByExpressionContext.isAsc() ? -1 : 1;
            _priorityQueue =
                    new IntHeapPriorityQueue(Math.min(limit, MAX_INITIAL_CAPACITY), (i1, i2) -> (i1 - i2) * comparisonFactor);
            _hasOrderBy = true;
        }
    }

    @Override
    protected IntermediateResultsBlock getNextBlock() {
        int limit = _distinctAggregationFunction.getLimit();
        String column = _distinctAggregationFunction.getInputExpressions().get(0).getIdentifier();

        assert _distinctAggregationFunction.getType() == AggregationFunctionType.DISTINCT;

        Dictionary dictionary = _dictionaryMap.get(column);
        int dictionarySize = dictionary.length();

        for (int dictId = 0; dictId < dictionarySize; dictId++) {
            if (!_hasOrderBy) {
                if (_dictIdSet.size() >= limit) {
                    break;
                }

                _dictIdSet.add(dictId);
            } else {
                if (!_dictIdSet.contains(dictId)) {
                    if (_dictIdSet.size() < limit) {
                        _dictIdSet.add(dictId);
                        _priorityQueue.enqueue(dictId);
                    } else {
                        int firstDictId = _priorityQueue.firstInt();
                        if (_priorityQueue.comparator().compare(dictId, firstDictId) > 0) {
                            _dictIdSet.remove(firstDictId);
                            _dictIdSet.add(dictId);
                            _priorityQueue.dequeueInt();
                            _priorityQueue.enqueue(dictId);
                        }
                    }
                }
            }
        }

        DistinctTable distinctTable = buildResult();

        return new IntermediateResultsBlock(new AggregationFunction[]{_distinctAggregationFunction},
                Collections.singletonList(distinctTable), false);
    }

    /**
     * Build the final result for this operation
     */
    private DistinctTable buildResult() {
        String column = _distinctAggregationFunction.getInputExpressions().get(0).getIdentifier();

        assert _distinctAggregationFunction.getType() == AggregationFunctionType.DISTINCT;

        Dictionary dictionary = _dictionaryMap.get(column);

        List<ExpressionContext> expressions = _distinctAggregationFunction.getInputExpressions();
        ExpressionContext expression = expressions.get(0);
        FieldSpec.DataType dataType = _transformOperator.getResultMetadata(expression).getDataType();

        DataSchema dataSchema = new DataSchema(new String[]{expression.toString()},
                new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.fromDataTypeSV(dataType)});
        List<Record> records = new ArrayList<>(_dictIdSet.size());
        Iterator<Integer> dictIdIterator = _dictIdSet.iterator();

        for(int i = 0; i < _dictIdSet.size(); i++) {
            records.add(new Record(new Object[]{dictionary.getInternal(dictIdIterator.next())}));
        }

        return new DistinctTable(dataSchema, records);
    }

    @Override
    public String getOperatorName() {
        return OPERATOR_NAME;
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
        // NOTE: Set numDocsScanned to numTotalDocs for backward compatibility.
        return new ExecutionStatistics(_numTotalDocs, 0, 0, _numTotalDocs);
    }
}
