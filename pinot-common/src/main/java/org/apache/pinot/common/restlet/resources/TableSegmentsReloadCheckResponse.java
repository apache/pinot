package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;


/**
 * This class gives list of the details from each server if there exists any segments that need to be reloaded
 *
 * It has details of reload flag which returns true if reload is needed on table and additional details of the
 * servers which need reload.
 */
public class TableSegmentsReloadCheckResponse {
  boolean _needReload;
  Map<String, SegmentsReloadCheckResponse> _serverToSegmentsReloadList;

  public Map<String, SegmentsReloadCheckResponse> getServerToSegmentsReloadList() {
    return _serverToSegmentsReloadList;
  }

  public boolean isNeedReload() {
    return _needReload;
  }

  public TableSegmentsReloadCheckResponse(@JsonProperty("needReload") boolean needReload,
      Map<String, SegmentsReloadCheckResponse> serverToSegmentsReloadList) {
    _needReload = needReload;
    _serverToSegmentsReloadList = serverToSegmentsReloadList;
  }
}
