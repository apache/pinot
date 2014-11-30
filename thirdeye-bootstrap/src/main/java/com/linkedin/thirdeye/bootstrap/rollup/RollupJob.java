package com.linkedin.thirdeye.bootstrap.rollup;

import java.io.FileInputStream;
import java.util.Properties;

import com.linkedin.thirdeye.bootstrap.aggregation.AggregatePhaseJob;

/**
 * Phase 1 Map <br/>
 * INPUT:{Dimension}{({time}{metrics})*} For each tuple, test if it satisfies
 * the roll up function, if yes: the entry passes through, write them to
 * separate directory. if no: generate all possible roll up as the key and
 * maintaining the original record Reduce <br/>
 * aggregate over the rolled up dimension and generate tuples in the following
 * format. {combination}(({time,metrics})* {list original dimensions that
 * contributed to this combination})
 * 
 * Phase 2 Map output: {original dimension}:{(time,metrics)* {combination}}
 * Reduce aggregate over all possible combinations {original
 * dimension}:{((time,metric)+,{combination})*} apply the roll up function and
 * select the right combination that passes the threshold. output: {rolled up
 * combination}{(time,metric) corresponding to the original dimension}
 * 
 * Phase 3: Map {rolled up combination}{(time,metric)* corresponding to the
 * original dimension} Reduce {rolled up combination}{(time,metric)* aggregated}
 * 
 * @author kgopalak
 * 
 */
public class RollupJob {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "usage: <phase(phase|phase2|phase3)> config.properties");
    }

    Properties props = new Properties();
    String phase = args[0];
    props.load(new FileInputStream(args[1]));
    if ("phase1".equalsIgnoreCase(phase)) {
      RollupPhaseOneJob job;
      job = new RollupPhaseOneJob("rollup_phase_one_job", props);
      job.run();
    }
  }
}
