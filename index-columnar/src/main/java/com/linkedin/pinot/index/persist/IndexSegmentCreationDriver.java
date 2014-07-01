package com.linkedin.pinot.index.persist;

import org.apache.commons.configuration.Configuration;


/**
 * This is the enter point for segment creation.
 * Initialize with setup properties as segmentCreationSpec then build Segment.
 * 
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class IndexSegmentCreationDriver {

  private static String SEGMENT_INDEX_TYPE = "segment.index.type";
  private static String SEGMENT_INDEX_VERSION = "segment.index.version";

  private Configuration _segmentCreationSpec = null;

  public void init(Configuration segmentCreationSpec) {
    _segmentCreationSpec = segmentCreationSpec;
  }

  public void build() throws Exception {
    DataReader _dataPublisher = DataReaderProvider.get(_segmentCreationSpec);
    IndexSegmentCreator indexSegmentCreator =
        IndexSegmentCreatorProvider.get(_segmentCreationSpec.getString(SEGMENT_INDEX_TYPE),
            _segmentCreationSpec.getString(SEGMENT_INDEX_VERSION));
    indexSegmentCreator.init(_segmentCreationSpec);
    while (_dataPublisher.hasNext()) {
      indexSegmentCreator.addRow(_dataPublisher.getNextIndexableRow());
    }
    indexSegmentCreator.buildSegment();
  }

}
