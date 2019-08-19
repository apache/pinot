package org.apache.pinot.core.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link IndexedTable} implementation for aggregating results
 */
public class AggregatorIndexedTable implements IndexedTable {

  private List<TableRecord> _records = new ArrayList<>(1);

  private List<AggregationFunction> _aggregationFunctions;

  private Lock _insertLock = new ReentrantLock();

  @Override
  public void init(DataSchema dataSchema, List<AggregationInfo> aggregationInfos, GroupBy groupBy,
      List<SelectionSort> orderBy, int maxCapacity) {
    _aggregationFunctions = new ArrayList<>(aggregationInfos.size());
    for (AggregationInfo aggregationInfo : aggregationInfos) {
      _aggregationFunctions.add(AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfo)
          .getAggregationFunction());
    }
  }

  /**
   * Aggregate the given value into the existing result
   */
  @Override
  public boolean upsert(TableRecord newRecord) {
    if (newRecord == null) {
      return false;
    }
    _insertLock.lock();
    try {
      if (size() == 0) {
        _records.add(newRecord);
      } else {
        TableRecord tableRecord = _records.get(0);
        for (int i = 0; i < _aggregationFunctions.size(); i++) {
          tableRecord._values[i] = _aggregationFunctions.get(i).merge(tableRecord._values[i], newRecord._values[i]);
        }
      }
    } finally {
      _insertLock.unlock();
    }
    return true;
  }

  /**
   * Aggregate the value from the given table into the existing table
   */
  @Override
  public boolean merge(IndexedTable table) {
    if (table == null) {
      return false;
    }
    Iterator<TableRecord> iterator = table.iterator();
    if (iterator.hasNext()) {
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
    // No sort in this case
    return true;
  }
}
