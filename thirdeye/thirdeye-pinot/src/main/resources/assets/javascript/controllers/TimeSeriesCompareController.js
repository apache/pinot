function TimeSeriesCompareController(parentController) {
  this.parentController = parentController;
  this.timeSeriesCompareModel = new TimeSeriesCompareModel();
  this.timeSeriesCompareView = new TimeSeriesCompareView(this.timeSeriesCompareModel);

  this.dimensionTreeMapController = new DimensionTreeMapController(this);

  // bind view events
  this.timeSeriesCompareView.heatmapRenderEvent.attach(this.handleHeatMapRenderEvent.bind(this));
}

TimeSeriesCompareController.prototype = {
  handleAppEvent: function (params) {
    params = params || HASH_SERVICE.getParams();
    this.timeSeriesCompareModel.init(params);
    this.timeSeriesCompareModel.update();
    this.timeSeriesCompareView.render();

    // render heatmap if view params are set in hashParams
    // this.dimensionTreeMapController.handleAppEvent(params);
  },

  handleHeatMapRenderEvent: function (viewObject) {

    // TODO: separate refreshWindowHash within update
    HASH_SERVICE.update(viewObject.viewParams);
    this.dimensionTreeMapController.handleAppEvent(HASH_SERVICE.getParams());
  }
};
