package com.linkedin.pinot.core.chunk.creator.impl;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.core.chunk.creator.ChunkIndexCreationDriver;
import com.linkedin.pinot.core.chunk.creator.ChunkPreIndexStatsCollector;
import com.linkedin.pinot.core.chunk.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.chunk.creator.ForwardIndexType;
import com.linkedin.pinot.core.chunk.creator.InvertedIndexType;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.generator.ChunkGeneratorConfiguration;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


/**
 * @author Dhaval Patel<dpatel@linkedin.com> Nov 6, 2014
 */

public class ChunkIndexCreationDriverImpl implements ChunkIndexCreationDriver {

  ChunkGeneratorConfiguration config;
  RecordReader recordReader;
  ChunkPreIndexStatsCollector statsCollector;
  Map<String, ColumnIndexCreationInfo> indexCreationInfoMap;
  ChunkColumnarIndexCreator indexCreator;
  Schema dataSchema;
  int totalDocs = 0;
  File tempIndexDir;

  @Override
  public void init(ChunkGeneratorConfiguration config) throws Exception {
    this.config = config;
    recordReader = RecordReaderFactory.get(config.getInputFileFormat(), config.getInputFilePath(), FieldExtractorFactory.get(config));
    recordReader.init();
    dataSchema = recordReader.getSchema();

    statsCollector = new ChunkPreIndexStatsCollectorImpl(recordReader.getSchema());
    statsCollector.init();
    indexCreationInfoMap = new HashMap<String, ColumnIndexCreationInfo>();
    indexCreator = new ChunkColumnarIndexCreator();
    final File indexDir = new File(config.getIndexOutputDir());
    if (!indexDir.exists()) {
      indexDir.mkdir();
    }
    tempIndexDir = new File(indexDir, com.linkedin.pinot.common.utils.FileUtils.getRandomFileName());
  }

  @Override
  public void build() throws Exception {

    while (recordReader.hasNext()) {
      totalDocs++;
      statsCollector.collectRow(recordReader.next());
    }

    buildIndexCreationInfo();

    indexCreator.init(config, indexCreationInfoMap, dataSchema, totalDocs, tempIndexDir);

    recordReader.rewind();

    while (recordReader.hasNext()) {
      indexCreator.index(recordReader.next());
    }

    recordReader.close();

    final File outputDir = new File(config.getIndexOutputDir());

    final String timeColumn = config.getTimeColumnName();
    final SegmentTimeUnit timeUnit = config.getTimeUnitForSegment();

    final Object minTimeValue = statsCollector.getColumnProfileFor(timeColumn).getMinValue();
    final Object maxTimeValue = statsCollector.getColumnProfileFor(timeColumn).getMaxValue();

    final String segmentName =
        SegmentNameBuilder.buildBasic(config.getResourceName(), config.getTableName(), minTimeValue, maxTimeValue,
            config.getSegmentNamePostfix());

    indexCreator.setSegmentName(segmentName);
    indexCreator.seal();
    FileUtils.moveDirectory(tempIndexDir, new File(outputDir, segmentName));
  }

  void buildIndexCreationInfo() throws Exception {
    statsCollector.build();
    for (final FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      final String column = spec.getName();
      indexCreationInfoMap.put(spec.getName(), new ColumnIndexCreationInfo(true, statsCollector.getColumnProfileFor(column).getMinValue(),
          statsCollector.getColumnProfileFor(column).getMaxValue(), statsCollector.getColumnProfileFor(column).getUniqueValuesSet(),
          ForwardIndexType.fixed_bit_compressed, InvertedIndexType.p4_delta, statsCollector.getColumnProfileFor(column).isSorted(),
          statsCollector.getColumnProfileFor(column).hasNull(), statsCollector.getColumnProfileFor(column).getTotalNumberOfEntries(), statsCollector.getColumnProfileFor(column).getMaxNumberOfMultiValues()));
    }
  }

}
