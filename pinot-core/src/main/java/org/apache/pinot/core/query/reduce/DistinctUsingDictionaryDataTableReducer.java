package org.apache.pinot.core.query.reduce;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.QueryOptions;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Reducer for reducing partial results for DISTINCT operation when being executed using dictionary based plan
 */
public class DistinctUsingDictionaryDataTableReducer implements DataTableReducer {
    private final DistinctAggregationFunction _distinctAggregationFunction;
    private final boolean _responseFormatSql;

    // TODO: queryOptions.isPreserveType() is ignored for DISTINCT queries.
    DistinctUsingDictionaryDataTableReducer(QueryContext queryContext, DistinctAggregationFunction distinctAggregationFunction) {
        _distinctAggregationFunction = distinctAggregationFunction;
        _responseFormatSql = new QueryOptions(queryContext.getQueryOptions()).isResponseFormatSQL();
    }

    /**
     * Reduces and sets results of distinct into
     * 1. ResultTable if _responseFormatSql is true
     * 2. SelectionResults by default
     */
    @Override
    public void reduceAndSetResults(String tableName, DataSchema dataSchema,
                                    Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
                                    DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
        // Gather all non-empty AbstractCollections
        List<AbstractCollection> nonEmptyAbstractCollections = new ArrayList<>(dataTableMap.size());

        for (DataTable dataTable : dataTableMap.values()) {
            AbstractCollection abstractCollection = dataTable.getObject(0, 0);
            if (abstractCollection.size() > 0) {
                nonEmptyAbstractCollections.add(abstractCollection);
            }
        }

        if (nonEmptyAbstractCollections.isEmpty()) {
            // All the AbstractCollections are empty, construct an empty response
            String[] columns = _distinctAggregationFunction.getColumns();
            if (_responseFormatSql) {
                int numColumns = columns.length;
                DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numColumns];
                Arrays.fill(columnDataTypes, DataSchema.ColumnDataType.STRING);
                brokerResponseNative
                        .setResultTable(new ResultTable(new DataSchema(columns, columnDataTypes), Collections.emptyList()));
            } else {
                brokerResponseNative.setSelectionResults(new SelectionResults(Arrays.asList(columns), Collections.emptyList()));
            }
        } else {
            // Construct a main AbstractCollection and merge all non-empty AbstractCollections into it
            AbstractCollection mainAbstractCollection = null;

            if (nonEmptyAbstractCollections.get(0) instanceof IntOpenHashSet) {
                mainAbstractCollection = new IntOpenHashSet();
            } else if (nonEmptyAbstractCollections.get(0) instanceof LongOpenHashSet) {
                mainAbstractCollection = new LongOpenHashSet();
            } else if (nonEmptyAbstractCollections.get(0) instanceof FloatOpenHashSet) {
                mainAbstractCollection = new FloatOpenHashSet();
            } else if (nonEmptyAbstractCollections.get(0) instanceof DoubleOpenHashSet) {
                mainAbstractCollection = new DoubleOpenHashSet();
            } else if (nonEmptyAbstractCollections.get(0) instanceof ObjectOpenHashSet) {
                mainAbstractCollection = new ObjectOpenHashSet();
            } else {
                throw new IllegalStateException();
            }

            assert mainAbstractCollection != null;

            for (AbstractCollection abstractCollection : nonEmptyAbstractCollections) {
                mainAbstractCollection.addAll(abstractCollection);
            }

            // Up until now, we have treated DISTINCT similar to another aggregation function even in terms
            // of the result from function and merging results.
            // However, the DISTINCT query is just another SELECTION style query from the user's point
            // of view and will return one or records in the result table for the column(s) selected and so
            // for that reason, response from broker should be a selection query result.
            if (_responseFormatSql) {
                brokerResponseNative.setResultTable(reduceToResultTable(mainAbstractCollection, dataSchema));
            } else {
                brokerResponseNative.setSelectionResults(reduceToSelectionResult(mainAbstractCollection, dataSchema));
            }
        }
    }

    private SelectionResults reduceToSelectionResult(AbstractCollection abstractCollection, DataSchema dataSchema) {
        List<Serializable[]> rows = new ArrayList<>(abstractCollection.size());
        DataSchema.ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
        int numColumns = columnDataTypes.length;
        Iterator<Record> iterator = abstractCollection.iterator();

        assert numColumns == 1;

        while (iterator.hasNext()) {
            Object value = iterator.next();
            Serializable[] row = new Serializable[numColumns];

            row[0] = (Serializable) value;

            rows.add(row);
        }
        return new SelectionResults(Arrays.asList(dataSchema.getColumnNames()), rows);
    }

    private ResultTable reduceToResultTable(AbstractCollection abstractCollection, DataSchema dataSchema) {
        List<Object[]> rows = new ArrayList<>(abstractCollection.size());
        DataSchema.ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
        int numColumns = columnDataTypes.length;
        Iterator<Record> iterator = abstractCollection.iterator();

        assert numColumns == 1;

        while (iterator.hasNext()) {
            Object value = iterator.next();
            Object[] row = new Object[numColumns];

            row[0] = value;

            rows.add(row);
        }
        return new ResultTable(dataSchema, rows);
    }
}
