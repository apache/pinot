package org.apache.pinot.core.data.manager.realtime;

import java.io.File;
import java.net.URI;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;


public class PeerSchemeSplitSegmentCommitter extends SplitSegmentCommitter {
  public PeerSchemeSplitSegmentCommitter(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      SegmentCompletionProtocol.Request.Params params, SegmentUploader segmentUploader) {
    super(segmentLogger, protocolHandler, params, segmentUploader);
  }

  // Always return true even if the segment upload fails and return null uri.
  // If the segment upload fails, put peer:///segment_name in the segment location to notify the controller it is a
  // peer download scheme.
  protected boolean uploadSegment(File segmentTarFile, SegmentUploader segmentUploader,
      SegmentCompletionProtocol.Request.Params params) {
    URI segmentLocation = segmentUploader.uploadSegment(segmentTarFile, new LLCSegmentName(params.getSegmentName()));
    if (segmentLocation != null) {
      params.withSegmentLocation(segmentLocation.toString());
    } else {
      params.withSegmentLocation("peer:///" + params.getSegmentName());
    }
    return true;
  }
}
