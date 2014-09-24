package com.linkedin.pinot.controller.helix.core;

import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;

import com.linkedin.pinot.controller.helix.api.PinotStandaloneResource;


public class PinotResourceIdealStateBuilder {
  public static final String ONLINE = "ONLINE";

  public static IdealState buildEmptyIdealStateFor(PinotStandaloneResource resource) {
    CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(resource.getResourceName());
    customModeIdealStateBuilder.setStateModel(PinotHelixStateModelGenerator.PINOT_HELIX_STATE_MODEL)
        .setNumPartitions(0).setNumReplica(0);
    IdealState idealState = customModeIdealStateBuilder.build();
    return idealState;
  }
}
