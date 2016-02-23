package com.linkedin.thirdeye.bootstrap.join;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.fs.FSDataInputStream;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.thirdeye.api.JoinSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;

public class JoinPhaseConfig {

  JoinSpec joinSpec;

  public JoinSpec getJoinSpec() {
    return joinSpec;
  }

  public void setJoinSpec(JoinSpec joinSpec) {
    this.joinSpec = joinSpec;
  }

  public static JoinPhaseConfig load(FSDataInputStream inputstream) throws IOException {
    return new ObjectMapper().readValue(inputstream, JoinPhaseConfig.class);
  }

  public static JoinPhaseConfig fromStarTreeConfig(StarTreeConfig starTreeConfig) {
    JoinPhaseConfig config = new JoinPhaseConfig();
    JoinSpec joinSpec = starTreeConfig.getJoinSpec();
    config.setJoinSpec(joinSpec);
    return config;
  }
}
