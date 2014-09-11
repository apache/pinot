package com.linkedin.pinot.core.query.pruner;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.config.SegmentPrunerConfig;


/**
 * An implementation of SegmentPrunerService.
 * 
 * @author xiafu
 *
 */
public class SegmentPrunerServiceImpl implements SegmentPrunerService {

  private static Logger LOGGER = LoggerFactory.getLogger(SegmentPrunerServiceImpl.class);
  private Set<SegmentPruner> _segmentPrunerSet;

  public SegmentPrunerServiceImpl(Set<SegmentPruner> segmentPrunerSet) {
    _segmentPrunerSet = segmentPrunerSet;
  }

  public SegmentPrunerServiceImpl(SegmentPrunerConfig prunerSetConfig) {
    _segmentPrunerSet = new HashSet<SegmentPruner>();
    if (prunerSetConfig != null) {
      for (int i = 0; i < prunerSetConfig.numberOfSegmentPruner(); ++i) {
        LOGGER.info("Adding SegmentPruner : " + prunerSetConfig.getSegmentPrunerName(i));
        _segmentPrunerSet.add(SegmentPrunerProvider.getSegmentPruner(prunerSetConfig.getSegmentPrunerName(i),
            prunerSetConfig.getSegmentPrunerConfig(i)));
      }
    }
  }

  @Override
  public boolean prune(IndexSegment segment, BrokerRequest brokerRequest) {
    //    if (_segmentPrunerSet == null || _segmentPrunerSet.size() == 0) {
    //      return false;
    //    }
    //
    //    for (SegmentPruner pruner : _segmentPrunerSet) {
    //      if (pruner.prune(segment, brokerRequest)) {
    //        return true;
    //      }
    //    }

    return false;
  }
}
