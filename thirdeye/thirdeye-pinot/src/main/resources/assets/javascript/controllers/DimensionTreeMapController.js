function DimensionTreeMapController() {
  this.dimensionTreeMapModel = new DimensionTreeMapModel();
  this.dimensionTreeMapView = new DimensionTreeMapView(this.dimensionTreeMapModel);
}

DimensionTreeMapController.prototype = {

  handleAppEvent : function() {
    console.log("----------------- rendering heatmap with hashParams ---------");
    console.log(HASH_SERVICE.getParams());
    this.dimensionTreeMapModel.init(HASH_SERVICE.getParams());
    this.dimensionTreeMapModel.update();
    this.dimensionTreeMapView.render();
  },
}
