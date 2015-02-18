package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.StarTreeManager;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ThirdEyeLifeCycleListener implements LifeCycle.Listener
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeLifeCycleListener.class);

  private final StarTreeManager starTreeManager;

  public ThirdEyeLifeCycleListener(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @Override
  public void lifeCycleStarting(LifeCycle lifeCycle)
  {
    // Do nothing
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
      for (String collection : starTreeManager.getCollections())
      {
        starTreeManager.close(collection);
      }
      LOG.info("Closed star tree manager");
    }
    catch (IOException e)
    {
      LOG.error("Caught exception while closing StarTree manager {}", e);
    }
  }

  @Override
  public void lifeCycleStopped(LifeCycle lifeCycle)
  {
    // Do nothing
  }
}
