package com.linkedin.thirdeye.views;

import com.linkedin.thirdeye.funnel.Funnel;
import io.dropwizard.views.View;

import java.util.List;

public class FunnelComponentView extends View
{
  private final List<Funnel> funnels;

  public FunnelComponentView(List<Funnel> funnels)
  {
    super("funnel-component.ftl");
    this.funnels = funnels;
  }

  public List<Funnel> getFunnels()
  {
    return funnels;
  }
}
