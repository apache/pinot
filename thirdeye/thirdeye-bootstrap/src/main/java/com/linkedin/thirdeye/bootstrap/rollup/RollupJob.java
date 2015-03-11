package com.linkedin.thirdeye.bootstrap.rollup;

import java.io.FileInputStream;
import java.util.Properties;

import com.linkedin.thirdeye.bootstrap.rollup.phase1.RollupPhaseOneJob;
import com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoJob;
import com.linkedin.thirdeye.bootstrap.rollup.phase3.RollupPhaseThreeJob;
import com.linkedin.thirdeye.bootstrap.rollup.phase4.RollupPhaseFourJob;

/**
 * <pre>
 * Phase 1 Map Only job
 * INPUT: {D}:{Map<T,M>}
 * OUTPUT: Input split into two directories aboveThreshold and belowThreshold
 * TODO: this optimization could have been done as part of previous aggregation
 * phase <br/>
 *
 * For each tuple, test if it satisfies the roll up
 * function, if yes: the entry passes through, write them to separate directory.
 * if no: generate all possible roll up as the key and maintaining the original
 * record Reduce <br/>
 *
 * ################################################
 *
 * Phase 2: Input <br/>
 *
 * Map phase
 *
 * Input: {D}:{Map<T,M>}
 * Output: {C}:{D, Map<T,M>}
 *
 *
 * Reduce {/code} aggregate over the rolled up dimension and generate tuples in
 * the following format.
 *
 * Input {C}: iterator <{D, Map<T,M>}>
 * Output{D}: { (C, Map<T,M>) (Map<T,M>)
 * Reduce phase output, key is the original rawDimensionKey, value consists of roll up combination two timeseries
 * 1. rollup combination C 2.original time series
 *
 * ################################################
 *
 * Phase 3
 * This is the phase where actual roll up happens
 * Map
 * This step partitions the input data by rawDimensionKey.
 * Reduce:
 * For each raw Dimension, we get all possible roll ups and select one combination to roll up
 * and output the selected roll up and the timeseries corresponding to raw dimension key
 * (note: timeseries corresponds to the raw Dimension D not the roll up}
 * Output: {C}:{Map<T,M>}
 * ################################################
 *
 * Phase 4:
 * This phase simply computes the distinct roll up and the aggregate time series.
 * The time series is used for debugging and some stat computation to
 * evaluate the effectiveness of roll up selection algorithm.
 *
 * NOTE: ALl these jobs could be more efficient by partitioning the output based on key and using map side join.
 * overall map reduce is not the right framework for doing this, its much more efficient to do it with something like spark
 *
 * </pre>
 *
 * @author kgopalak
 *
 */
public class RollupJob {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "usage: <phase(phase1|phase2|phase3|phase4)> config.properties");
    }

    Properties props = new Properties();
    String phase = args[0];
    props.load(new FileInputStream(args[1]));
    if ("phase1".equalsIgnoreCase(phase)) {
      RollupPhaseOneJob job;
      job = new RollupPhaseOneJob("rollup_phase_one_job", props);
      job.run();
    }

    if ("phase2".equalsIgnoreCase(phase)) {
      RollupPhaseTwoJob job;
      job = new RollupPhaseTwoJob("rollup_phase_two_job", props);
      job.run();
    }
    if ("phase3".equalsIgnoreCase(phase)) {
      RollupPhaseThreeJob job;
      job = new RollupPhaseThreeJob("rollup_phase_three_job", props);
      job.run();
    }
    if ("phase4".equalsIgnoreCase(phase)) {
      RollupPhaseFourJob job;
      job = new RollupPhaseFourJob("rollup_phase_four_job", props);
      job.run();
    }
  }
}
