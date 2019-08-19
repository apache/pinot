package org.apache.pinot.core.data;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link IndexedTable} implementation for appending results
 * e.g. Selection operation
 */
public class AppenderIndexedTable implements IndexedTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AppenderIndexedTable.class);

  private List<TableRecord> _records = new ArrayList<>();

  private DataSchema _dataSchema;
  private List<SelectionSort> _orderBy;
  private int _maxCapacity;

  @Override
  public void init(DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int maxCapacity) {
    _dataSchema = dataSchema;
    _orderBy = orderBy;
    _maxCapacity = maxCapacity;
  }

  /**
   * Add the new record to the collection
   */
  @Override
  public boolean upsert(TableRecord newRecord) {
    if (newRecord == null) {
      return false;
    }
    if (size() >= _maxCapacity) {
      LOGGER.debug("Could not add newRecord {}. Reached maxCapacity {}", newRecord, _maxCapacity);
      return false;
    }
    _records.add(newRecord);
    return true;
  }

  /**
   * Add all records of given table into existing table
   */
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
    Comparator<IndexedTable.TableRecord> comparator = OrderByUtils.getValuesComparator(_dataSchema, _orderBy);
    _records.sort(comparator);
    return true;
  }
}
