package org.apache.pinot.controller.helix.core.listener;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.model.InstanceConfig;

import java.util.ArrayList;
import java.util.List;

public class ClusterInstanceConfigChangeListener implements InstanceConfigChangeListener {
    private List<InstanceConfig> _instanceConfigs = new ArrayList<>();

    public ClusterInstanceConfigChangeListener() {
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
        _instanceConfigs = instanceConfigs;
    }

    public List<InstanceConfig> getInstanceConfigs() {
        return _instanceConfigs;
    }
}
