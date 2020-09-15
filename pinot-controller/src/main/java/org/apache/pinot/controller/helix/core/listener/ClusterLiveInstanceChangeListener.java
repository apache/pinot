package org.apache.pinot.controller.helix.core.listener;

import java.util.ArrayList;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.model.LiveInstance;

import java.util.List;
import org.apache.helix.PropertyKey.Builder;


public class ClusterLiveInstanceChangeListener implements LiveInstanceChangeListener {
  private HelixDataAccessor _helixDataAccessor;
  private Builder _keyBuilder;
  private List<LiveInstance> _liveInstances = new ArrayList<>();

  public ClusterLiveInstanceChangeListener(HelixDataAccessor helixDataAccessor, Builder keyBuilder) {
    _helixDataAccessor = helixDataAccessor;
    _keyBuilder = keyBuilder;
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
    _liveInstances = liveInstances;
  }

  public List<LiveInstance> getLiveInstances() {
    if (_liveInstances.isEmpty()) {
      _liveInstances = _helixDataAccessor.getProperty(_keyBuilder.liveInstances());
    }
    return _liveInstances;
  }
}
