package com.linkedin.pinot.core.segment.creator.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.segment.creator.AbstractColumnPreIndexStatsCollector;
import com.linkedin.pinot.core.segment.creator.SegmentPreIndexStatsCollector;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 7, 2014
 */

public class SegmentPreIndexStatsCollectorImpl implements SegmentPreIndexStatsCollector {
  private static final Logger logger = Logger.getLogger(SegmentPreIndexStatsCollectorImpl.class);

  private final Schema dataSchema;
  Map<String, AbstractColumnPreIndexStatsCollector> columnStatsCollectorMap;

  public SegmentPreIndexStatsCollectorImpl(Schema dataSchema) {
    this.dataSchema = dataSchema;
  }

  @Override
  public void init() {
    columnStatsCollectorMap = new HashMap<String, AbstractColumnPreIndexStatsCollector>();

    for (final FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      switch (spec.getDataType()) {
        case BOOLEAN:
        case STRING:
          columnStatsCollectorMap.put(spec.getName(), new StringColumnPreIndexStatsCollector(spec));
          break;
        case INT:
          columnStatsCollectorMap.put(spec.getName(), new IntColumnPreIndexStatsCollector(spec));
          break;
        case LONG:
          columnStatsCollectorMap.put(spec.getName(), new LongColumnPreIndexStatsCollector(spec));
          break;
        case FLOAT:
          columnStatsCollectorMap.put(spec.getName(), new FloatColumnPreIndexStatsCollector(spec));
          break;
        case DOUBLE:
          columnStatsCollectorMap.put(spec.getName(), new DoubleColumnPreIndexStatsCollector(spec));
          break;
        default:
          break;
      }
    }
  }

  @Override
  public void build() throws Exception {
    for (final String column : columnStatsCollectorMap.keySet()) {
      columnStatsCollectorMap.get(column).seal();
    }
  }

  @Override
  public AbstractColumnPreIndexStatsCollector getColumnProfileFor(String column) throws Exception {
    return columnStatsCollectorMap.get(column);
  }

  @Override
  public void collectRow(GenericRow row) throws Exception {
    for (final String column : row.getFieldNames()) {
      columnStatsCollectorMap.get(column).collect(row.getValue(column));
    }
  }

  public static <T> T convertInstanceOfObject(Object o, Class<T> clazz) {
    try {
      return clazz.cast(o);
    } catch (final ClassCastException e) {
      return null;
    }
  }

  @Override
  public void logStats() {
    try {
      for (final String column : columnStatsCollectorMap.keySet()) {
        logger.info("********** logging for column : " + column + " ********************* ");
        logger.info("min value : " + columnStatsCollectorMap.get(column).getMinValue());
        logger.info("max value : " + columnStatsCollectorMap.get(column).getMaxValue());
        logger.info("cardinality : " + columnStatsCollectorMap.get(column).getCardinality());
        logger.info("length of largest column : " + columnStatsCollectorMap.get(column).getLengthOfLargestElement());
        logger.info("is sorted : " + columnStatsCollectorMap.get(column).isSorted());
        logger.info("column type : " + dataSchema.getFieldSpecFor(column).getDataType());
        logger.info("***********************************************");
      }
    } catch (final Exception e) {
      logger.error(e.getMessage());
    }

  }
}
