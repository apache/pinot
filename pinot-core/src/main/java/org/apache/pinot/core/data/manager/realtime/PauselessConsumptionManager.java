package org.apache.pinot.core.data.manager.realtime;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// FIXME add a description
public class PauselessConsumptionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PauselessConsumptionManager.class);

  private final Map<String, String> _committingSegmentToHiddenConsumingSegmentMap = new ConcurrentHashMap<>();

  public PauselessConsumptionManager() {
  }

  public void addAssociation(String committingSegment, String hiddenConsumingSegment) {
    _committingSegmentToHiddenConsumingSegmentMap.put(committingSegment, hiddenConsumingSegment);
  }

  public boolean removeAssociationForHiddenConsumingSegment(String targetHiddenConsumingSegment) {
    StringBuilder committingSegmentName = new StringBuilder();
    _committingSegmentToHiddenConsumingSegmentMap.forEach((committingSegment, hiddenConsumingSegment) -> {
      if (targetHiddenConsumingSegment.equals(hiddenConsumingSegment)) {
        committingSegmentName.append(committingSegment);
      }
    });
    if (committingSegmentName.length() > 0) {
      _committingSegmentToHiddenConsumingSegmentMap.remove(committingSegmentName.toString());
      // TODO: add proper log
      return true;
    }
    return false;
  }

  public boolean removeAssociationForCommittingSegment(String committingSegment) {
    return _committingSegmentToHiddenConsumingSegmentMap.remove(committingSegment) != null;
  }

  public void updateSegmentsToQuery(List<String> segmentsToQuery, List<String> optionalSegments) {
    Set<String> segmentsToBeAdded = new HashSet<>();
    for (String segmentToQuery : segmentsToQuery) {
      String newConsumingSegment = _committingSegmentToHiddenConsumingSegmentMap.get(segmentToQuery);
      if (newConsumingSegment != null) {
        segmentsToBeAdded.add(newConsumingSegment);
      }
    }
    if (optionalSegments != null) {
      optionalSegments.forEach(segmentsToBeAdded::remove);
    }
    segmentsToQuery.addAll(segmentsToBeAdded);
  }

  public String getExistingAssociatedSegment(String committingSegment) {
    return _committingSegmentToHiddenConsumingSegmentMap.get(committingSegment);
  }
}
