package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.List;

/**
 * This class keeps the client configs for all the clients used in thirdeye
 */
public class ClientConfig {

  private List<Client> clients = new ArrayList<>();

  public List<Client> getClients() {
    return clients;
  }
  public void setClients(List<Client> clients) {
    this.clients = clients;
  }

}
