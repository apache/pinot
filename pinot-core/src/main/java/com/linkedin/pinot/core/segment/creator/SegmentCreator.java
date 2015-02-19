package com.linkedin.pinot.core.segment.creator;

import com.linkedin.pinot.core.data.GenericRow;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.commons.configuration.ConfigurationException;


/**
 * Interface for segment creators, which create an index over a set of rows and writes the resulting index to disk.
 *
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 21, 2014
 */

public interface SegmentCreator {

  /**
   * Initializes the segment creation.
   *
   * @param segmentCreationSpec
   * @param indexCreationInfoMap
   * @param schema
   * @param totalDocs
   * @param outDir
   * @throws Exception
   */
  void init(SegmentGeneratorConfig segmentCreationSpec, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema,
      int totalDocs, File outDir) throws Exception;

  /**
   * Adds a row to the index.
   *
   * @param row The row to index.
   */
  void indexRow(GenericRow row);

  /**
   * Sets the name of the segment.
   *
   * @param segmentName The name of the segment
   */
  void setSegmentName(String segmentName);

  /**
   * Seals the segment, flushing it to disk.
   *
   * @throws ConfigurationException
   * @throws IOException
   */
  void seal() throws ConfigurationException, IOException;
}
