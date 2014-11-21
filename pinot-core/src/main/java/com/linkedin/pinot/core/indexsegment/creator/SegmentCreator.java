package com.linkedin.pinot.core.indexsegment.creator;

import java.io.File;
import java.util.Map;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.chunk.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;


/**
 * Initialized with fieldSpec.
 * Call addRow(...) to index row events.
 * After finished adding, call buildSegment() to create a segment.
 *
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface SegmentCreator {

  public void init(SegmentGeneratorConfig segmentCreationSpec, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema,
      int totalDocs, File outDir) throws Exception;

  public IndexSegment buildSegment() throws Exception;

}
