package com.linkedin.pinot.query.pruner;

import java.util.HashSet;
import java.util.Set;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.query.config.SegmentPrunerConfig;
import com.linkedin.pinot.query.request.Query;


/**
 * An implementation of SegmentPrunerService.
 * 
 * @author xiafu
 *
 */
public class SegmentPrunerServiceImpl implements SegmentPrunerService {
  private Set<SegmentPruner> _segmentPrunerSet;

  public SegmentPrunerServiceImpl(Set<SegmentPruner> segmentPrunerSet) {
    _segmentPrunerSet = segmentPrunerSet;
  }

  public SegmentPrunerServiceImpl(SegmentPrunerConfig prunerSetConfig) {
    _segmentPrunerSet = new HashSet<SegmentPruner>();
    if (prunerSetConfig != null) {
      for (int i = 0; i < prunerSetConfig.numberOfSegmentPruner(); ++i) {
        _segmentPrunerSet.add(SegmentPrunerProvider.getSegmentPruner(prunerSetConfig.getSegmentPrunerName(i),
            prunerSetConfig.getSegmentPrunerConfig(i)));
      }
    }
  }

  @Override
  public boolean prune(IndexSegment segment, Query query) {
    if (_segmentPrunerSet == null || _segmentPrunerSet.size() == 0) {
      return false;
    }

    for (SegmentPruner pruner : _segmentPrunerSet) {
      if (pruner.prune(segment, query)) {
        return true;
      }
    }

    return false;
  }
}
