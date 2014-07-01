package com.linkedin.pinot.server.partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A collection of Segments with same table name.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class TableDataManager {

  private final Map<String, SegmentDataManager> _segmentDataManagerMap = new HashMap<String, SegmentDataManager>();
  private final List<String> activeSegments = new ArrayList<String>();

  public TableDataManager() {

  }

  public Collection<SegmentDataManager> getSegmentDataManagerList() {
    return _segmentDataManagerMap.values();
  }

  public synchronized void addSegment(SegmentDataManager segmentDataManager) {
    _segmentDataManagerMap.put(segmentDataManager.getSegment().getSegmentName(), segmentDataManager);
    activeSegments.add(segmentDataManager.getSegment().getSegmentName());
  }

  public synchronized void removeSegment(String segmentName) {
    _segmentDataManagerMap.remove(segmentName);
  }
}
