function TimeSeriesCompareController(parentController) {
  this.parentController = parentController;
  this.timeSeriesCompareModel = new TimeSeriesCompareModel();
  this.timeSeriesCompareView = new TimeSeriesCompareView(this.timeSeriesCompareModel);

  this.dimensionTreeMapController = new DimensionTreeMapController(this);

  // bind view events
  this.timeSeriesCompareView.heatmapRenderEvent.attach(this.handleHeatMapRenderEvent.bind(this));
}

TimeSeriesCompareController.prototype = {
  handleAppEvent: function (hashParams) {
    this.timeSeriesCompareModel.init(hashParams);
    this.timeSeriesCompareModel.update();
    this.timeSeriesCompareView.render();

    // render heatmap if view params are set in hashParams
    this.dimensionTreeMapController.handleAppEvent(hashParams);
  },

  handleHeatMapRenderEvent: function (viewObject) {
    console.log(HASH_SERVICE.getParams());

    // update hash params as per parameters set in the view
    HASH_SERVICE.update(viewObject.viewParams);
    this.dimensionTreeMapController.handleAppEvent(HASH_SERVICE.getParams());
  }
};
