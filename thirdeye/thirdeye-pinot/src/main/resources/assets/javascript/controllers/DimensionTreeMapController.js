function DimensionTreeMapController() {
  this.dimensionTreeMapModel = new DimensionTreeMapModel();
  this.dimensionTreeMapView = new DimensionTreeMapView(this.dimensionTreeMapModel);
}

DimensionTreeMapController.prototype = {

  handleAppEvent : function(params) {
    params = params || HASH_SERVICE.getParams();
    this.dimensionTreeMapModel.init(params);
    this.dimensionTreeMapModel.update();
    this.dimensionTreeMapView.render();
  },
}
