package com.linkedin.pinot.broker.broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.apache.commons.configuration.PropertiesConfiguration;


public class TestBrokerServerBuilder {

  public static void main(String[] args) throws Exception {
    PropertiesConfiguration config =
        new PropertiesConfiguration(new File(
            "/home/dpatel/experiments/github/pinot/pinot-broker/src/test/resources/broker.properties"));
    final BrokerServerBuilder bld = new BrokerServerBuilder(config);
    bld.buildNetwork();
    bld.buildHTTP();
    bld.start();
    
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          bld.stop();
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    });

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      String command = br.readLine();
      if (command.equals("exit")) {
        bld.stop();
      }
    }

  }
}
