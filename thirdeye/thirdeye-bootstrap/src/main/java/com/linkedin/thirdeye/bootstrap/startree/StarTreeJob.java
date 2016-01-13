package com.linkedin.thirdeye.bootstrap.startree;

import java.io.FileInputStream;
import java.util.Properties;

import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneJob;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoJob;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationJob;

/**
 * @author kgopalak
 */
public class StarTreeJob {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "usage: <phase(generation|bootstrap|update)> config.properties");
    }

    Properties props = new Properties();
    String phase = args[0];
    props.load(new FileInputStream(args[1]));
    if ("generation".equalsIgnoreCase(phase)) {
      StarTreeGenerationJob job;
      job = new StarTreeGenerationJob("star_tree_generation_job", props);
      job.run();
    }
    if ("bootstrap_phase1".equalsIgnoreCase(phase)) {
      StarTreeBootstrapPhaseOneJob job;
      job = new StarTreeBootstrapPhaseOneJob("star_tree_bootstrap_phase1_job", props);
      job.run();
    }
    if ("bootstrap_phase2".equalsIgnoreCase(phase)) {
      StarTreeBootstrapPhaseTwoJob job;
      job = new StarTreeBootstrapPhaseTwoJob("star_tree_bootstrap_phase2_job", props);
      job.run();
    }
  }
}
