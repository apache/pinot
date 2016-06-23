package com.linkedin.thirdeye.dashboard.views;

public interface ViewHandler<Request extends ViewRequest, Response extends ViewResponse> {

  Response process(Request request) throws Exception;

}
