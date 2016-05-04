package com.linkedin.thirdeye.dashboard.views;


import com.linkedin.thirdeye.dashboard.resources.ViewRequestParams;

public interface ViewHandler<Request extends ViewRequest, Response extends ViewResponse> {

  ViewRequest createRequest(ViewRequestParams ViewRequesParams);
  
  Response process(Request request) throws Exception;

}
