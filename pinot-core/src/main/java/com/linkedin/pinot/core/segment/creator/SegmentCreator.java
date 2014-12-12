package com.linkedin.pinot.core.segment.creator;

import java.io.File;
import java.util.Map;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 21, 2014
 */

public interface SegmentCreator {

  void init(SegmentGeneratorConfig segmentCreationSpec, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema,
      int totalDocs, File outDir) throws Exception;

}
