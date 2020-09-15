package org.apache.pinot.controller.helix.core.listener;

import java.util.ArrayList;
import java.util.List;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.model.ExternalView;


public class ClusterExternalViewChangeListener implements ExternalViewChangeListener {
  private List<ExternalView> _externalViewList = new ArrayList<>();

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    _externalViewList = externalViewList;
  }

  public List<ExternalView> getExternalViewList() {
    return _externalViewList;
  }
}
