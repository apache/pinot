package org.apache.pinot.controller.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


public class Instances {
  List<String> _instances;

  public Instances(@JsonProperty("instances") List<String> instances) {
    _instances = instances;
  }

  public List<String> getInstances() {
    return _instances;
  }
}
