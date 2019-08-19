package org.apache.pinot.core.data;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link IndexedTable} implementation for combining TableRecords based on combination of keys
 */
public class ReducerIndexedTable implements IndexedTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReducerIndexedTable.class);

  private List<TableRecord> _records = new ArrayList<>();

  private DataSchema _dataSchema;
  private List<AggregationInfo> _aggregationInfos;
  private List<AggregationFunction> _aggregationFunctions;
  private List<SelectionSort> _orderBy;
  private int _maxCapacity;

  private ConcurrentMap<Object[], Integer> _lookupTable;

  @Override
  public void init(DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int maxCapacity) {
    _dataSchema = dataSchema;
    _aggregationInfos = aggregationInfos;
    _orderBy = orderBy;
    _maxCapacity = maxCapacity;

    _lookupTable = new ConcurrentHashMap<>(maxCapacity);

    if (CollectionUtils.isNotEmpty(aggregationInfos)) {
      _aggregationFunctions = new ArrayList<>(aggregationInfos.size());
      for (AggregationInfo aggregationInfo : aggregationInfos) {
        _aggregationFunctions.add(
            AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfo).getAggregationFunction());
      }
    }
  }

  /**
   * Aggregate the results into the record with the same key
   */
  @Override
  public boolean upsert(TableRecord newRecord) {
    if (newRecord == null) {
      return false;
    }

    Object[] keys = newRecord._keys;
    Preconditions.checkNotNull(keys, "ReducerIndexedTable cannot accept TableRecords with null keys");

    _lookupTable.compute(keys, (k, index) -> {
      if (index == null) {
        if (size() >= _maxCapacity) {
          LOGGER.debug("Could not add new record {}. Reached max capacity {}", newRecord, _maxCapacity);
        } else {
          index = size();
          _records.add(newRecord);
        }
      } else {
        TableRecord existingRecord = _records.get(index);
        if (CollectionUtils.isNotEmpty(_aggregationFunctions)) {
          for (int i = 0; i < _aggregationFunctions.size(); i++) {
            existingRecord._values[i] =
                _aggregationFunctions.get(i).merge(existingRecord._values[i], newRecord._values[i]);
          }
        }
      }
      return index;
    });
    return true;
  }

  @Override
  public boolean merge(IndexedTable table) {
    if (table == null) {
      return false;
    }
    Iterator<TableRecord> iterator = table.iterator();
    while (iterator.hasNext() && size() < _maxCapacity) {
      upsert(iterator.next());
    }
    return true;
  }

  @Override
  public int size() {
    return _records.size();
  }

  @Override
  public Iterator<TableRecord> iterator() {
    return _records.iterator();
  }

  @Override
  public boolean sort() {
    Comparator<IndexedTable.TableRecord> comparator;
    if (CollectionUtils.isNotEmpty(_aggregationFunctions)) {
      comparator = OrderByUtils.getKeysAndValuesComparator(_dataSchema, _orderBy, _aggregationInfos);
    } else {
      comparator = OrderByUtils.getKeysComparator(_dataSchema, _orderBy);
    }
    _records.sort(comparator);
    return true;
  }
}
