package org.apache.pinot.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ControllerBrokerSelector implements BrokerSelector {
    private static final Random RANDOM = new Random();
    private List<String> _brokers;
    private String _controllerAddress;
    private BrokerUpdater _brokerUpdater;

    private class BrokerUpdater extends Thread {
        @Override
        public void run() {
            while (true) {
                // Periodically update _brokers
            }
        }
    }

    public ControllerBrokerSelector(String controllerAddress) {
        this._controllerAddress = controllerAddress;
        this._brokers = new ArrayList<>();
        this._brokerUpdater = new BrokerUpdater();
        _brokerUpdater.run();
        _brokerUpdater.start();
    }

    @Override
    public String selectBroker(String table) {
        return _brokers.get(RANDOM.nextInt(_brokers.size()));
    }

    @Override
    public List<String> getBrokers() {
        return _brokers;
    }

    @Override
    public void close() {
        // stop updaterThread? Run it via an executor?
    }
}
