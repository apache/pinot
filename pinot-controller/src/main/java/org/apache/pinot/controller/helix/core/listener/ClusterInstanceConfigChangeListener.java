package org.apache.pinot.controller.helix.core.listener;

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.model.InstanceConfig;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.helix.HelixHelper;


public class ClusterInstanceConfigChangeListener implements InstanceConfigChangeListener {
    private HelixManager _helixManager;
    private List<InstanceConfig> _instanceConfigs = new ArrayList<>();

    public ClusterInstanceConfigChangeListener(HelixManager helixManager) {
        _helixManager = helixManager;
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
        _instanceConfigs = instanceConfigs;
    }

    public List<InstanceConfig> getInstanceConfigs() {
        if(_instanceConfigs.isEmpty()){
            _instanceConfigs = HelixHelper.getInstanceConfigs(_helixManager);
        }
        return _instanceConfigs;
    }
}
