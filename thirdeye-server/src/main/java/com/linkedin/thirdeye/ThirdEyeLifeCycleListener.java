package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.StarTreeManager;
import org.apache.helix.HelixManager;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ThirdEyeLifeCycleListener implements LifeCycle.Listener
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeLifeCycleListener.class);

  private final HelixManager helixManager;
  private final StarTreeManager starTreeManager;

  public ThirdEyeLifeCycleListener(HelixManager helixManager, StarTreeManager starTreeManager)
  {
    this.helixManager = helixManager;
    this.starTreeManager = starTreeManager;
  }

  @Override
  public void lifeCycleStarting(LifeCycle lifeCycle)
  {
    if (helixManager != null)
    {
      try
      {
        helixManager.connect();
        LOG.info("Connected Helix manager");
      }
      catch (Exception e)
      {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  public void lifeCycleStarted(LifeCycle lifeCycle)
  {
    // Do nothing
  }

  @Override
  public void lifeCycleFailure(LifeCycle lifeCycle, Throwable throwable)
  {
    // Do nothing
  }

  @Override
  public void lifeCycleStopping(LifeCycle lifeCycle)
  {
    try
    {
      starTreeManager.close();
      LOG.info("Closed star tree manager");
    }
    catch (IOException e)
    {
      LOG.error("Caught exception while closing StarTree manager {}", e);
    }

    if (helixManager != null)
    {
      try
      {
        helixManager.disconnect();
        LOG.info("Disconnected Helix manager");
      }
      catch (Exception e)
      {
        LOG.error("Caught exception while closing Helix manager {}", e);
      }
    }
  }

  @Override
  public void lifeCycleStopped(LifeCycle lifeCycle)
  {
    // Do nothing
  }
}
