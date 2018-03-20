package com.linkedin.pinot.core.data.readers.sort;

import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;
import com.linkedin.pinot.core.startree.DimensionBuffer;
import com.linkedin.pinot.core.startree.MetricBuffer;
import com.linkedin.pinot.core.startree.OffHeapStarTreeBuilder;
import com.linkedin.pinot.core.startree.StarTreeBuilderConfig;
import com.linkedin.pinot.core.startree.StarTreeDataTable;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;

public class OffHeapPinotSegmentSorter extends OffHeapStarTreeBuilder implements SegmentSorter {

  private int _numDocs;
  private Schema _schema;
  private Map<String, PinotSegmentColumnReader> _columnReaderMap;
  private File _tempDir;

  public OffHeapPinotSegmentSorter(int numDocs, Schema schema, Map<String, PinotSegmentColumnReader> columnReaderMap, File tempParent ) {
    _numDocs = numDocs;
    _schema = schema;
    _columnReaderMap = columnReaderMap;
    _tempDir = new File(tempParent, com.linkedin.pinot.common.utils.FileUtils.getRandomFileName());
  }

  @Override
  public int[] getSortedDocIds(List<String> sortOrder) throws IOException {
    StarTreeBuilderConfig config=  new StarTreeBuilderConfig();
    config.setDimensionsSplitOrder(sortOrder);
    config.setSchema(_schema);

    init(config);

    _sortOrder = new int[_dimensionNames.size()];
    Set<Integer> covered = new HashSet<>();
    int index = 0;
    for (String dimension : sortOrder) {
      int dimensionId = _dimensionNames.indexOf(dimension);
      if (dimensionId != -1) {
        _sortOrder[index++] = dimensionId;
        covered.add(dimensionId);
      }
    }

    for(int i = 0; i < _dimensionNames.size(); i++) {
      if (!covered.contains(i)) {
        _sortOrder[index++] = i;
      }
    }

    System.out.println("Sort order: "  + Arrays.toString(_sortOrder));


    for (int i = 0; i < _numDocs; i++) {
      append(i);
    }
    _dataOutputStream.flush();

    int[] result;
    try (StarTreeDataTable dataTable = new StarTreeDataTable(new MMapBuffer(_dataFile, MMapMode.READ_WRITE),
        _dimensionSize, _metricSize, 0, _numRawDocs)) {
      result = dataTable.sortedDocIds(0, _numRawDocs, _sortOrder);
    }

    return result;
  }

  public void append(int docId) throws IOException {
    // Dimensions
    DimensionBuffer dimensions = new DimensionBuffer(_numDimensions);
    for (int i = 0; i < _numDimensions; i++) {
      String dimensionName = _dimensionNames.get(i);
      int dictId = _columnReaderMap.get(dimensionName).getDictionaryId(docId);
      dimensions.setDimension(i, dictId);
    }

    // Metrics
    Object[] metricValues = new Object[_numMetrics];
    List<MetricFieldSpec> metricFieldSpecs = _schema.getMetricFieldSpecs();
    for (int i = 0; i < _numMetrics; i++) {
      String metricName = _metricNames.get(i);
      MetricFieldSpec fieldSpec = metricFieldSpecs.get(i);
      Object metricValue = getMetricValue(docId,fieldSpec,_columnReaderMap.get(metricName));

      if (metricFieldSpecs.get(i).getDerivedMetricType() == MetricFieldSpec.DerivedMetricType.HLL) {
        // Convert HLL field from string format to HyperLogLog
        metricValues[i] = HllUtil.convertStringToHll((String) metricValue);
      } else {
        // No conversion for standard data types
        metricValues[i] = metricValue;
      }
    }
    MetricBuffer metrics = new MetricBuffer(metricValues, metricFieldSpecs);

    appendToRawBuffer(dimensions, metrics);
  }


  public Object getMetricValue(int docId, MetricFieldSpec fieldSpec, PinotSegmentColumnReader columnReader) {
    switch (fieldSpec.getDataType()) {
      case INT:
        return columnReader.readInt(docId);
      case LONG:
        return columnReader.readLong(docId);
      case FLOAT:
        return columnReader.readFloat(docId);
      case DOUBLE:
        return columnReader.readDouble(docId);
      case STRING:
        return columnReader.readString(docId);
      default:
        throw new IllegalStateException(
            "Field: " + fieldSpec.getName() + " has illegal data type: " + fieldSpec.getDataType());
    }
  }
}
