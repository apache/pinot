function TimeSeriesCompareController(parentController) {
  this.parentController = parentController;
  this.timeSeriesCompareModel = new TimeSeriesCompareModel();
  this.timeSeriesCompareView = new TimeSeriesCompareView(this.timeSeriesCompareModel);

  this.dimensionTreeMapController = new DimensionTreeMapController(this);

  // bind view events
  this.timeSeriesCompareView.heatmapRenderEvent.attach(this.handleHeatMapRenderEvent.bind(this));
}

TimeSeriesCompareController.prototype = {
  handleAppEvent: function () {
    this.timeSeriesCompareModel.init(HASH_SERVICE.getParams());
    this.timeSeriesCompareModel.update();
    this.timeSeriesCompareView.render();

    // render heatmap if view params are set in hashParams
    this.dimensionTreeMapController.handleAppEvent();
  },

  handleHeatMapRenderEvent: function (viewObject) {
    console.log(HASH_SERVICE.getParams());

    // TODO: separate refreshWindowHash within update
    HASH_SERVICE.update(viewObject.viewParams);
    this.dimensionTreeMapController.handleAppEvent(HASH_SERVICE.getParams());
  }
};
