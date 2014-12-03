package com.linkedin.thirdeye.cluster;

import com.linkedin.thirdeye.api.StarTreeManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThirdEyeTransitionHandler extends TransitionHandler
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeTransitionHandler.class);

  private final PartitionId partitionId;
  private final StarTreeManager starTreeManager;

  public ThirdEyeTransitionHandler(PartitionId partitionId, StarTreeManager starTreeManager)
  {
    this.partitionId = partitionId;
    this.starTreeManager = starTreeManager;
  }

  @Transition(from = "OFFLINE", to = "ONLINE")
  public void fromOfflineToOnline(Message message, NotificationContext context) throws Exception
  {
    LOG.info("BEGIN\t{}: OFFLINE -> ONLINE", message.getPartitionId());
    LOG.info("END\t{}: OFFLINE -> ONLINE", message.getPartitionId());
  }

  @Transition(from = "ONLINE", to = "OFFLINE")
  public void fromOnlineToOffline(Message message, NotificationContext context) throws Exception
  {
    LOG.info("BEGIN\t{}: ONLINE -> OFFLINE", message.getPartitionId());
    LOG.info("END\t{}: ONLINE -> OFFLINE", message.getPartitionId());
  }

  @Transition(from = "OFFLINE", to = "DROPPED")
  public void fromOfflineToDropped(Message message, NotificationContext context) throws Exception
  {
    LOG.info("BEGIN\t{}: OFFLINE -> DROPPED", message.getPartitionId());
    LOG.info("END\t{}: OFFLINE -> DROPPED", message.getPartitionId());
  }
}
