package org.apache.pinot.controller.helix.core.listener;

import java.util.ArrayList;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.model.LiveInstance;

import java.util.List;

public class ClusterLiveInstanceChangeListener implements LiveInstanceChangeListener {
    private List<LiveInstance> _liveInstances = new ArrayList<>();

    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
        _liveInstances = liveInstances;
    }

    public List<LiveInstance> getLiveInstances() {
        return _liveInstances;
    }
}
