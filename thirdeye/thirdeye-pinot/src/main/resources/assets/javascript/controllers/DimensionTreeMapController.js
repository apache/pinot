function DimensionTreeMapController() {
  this.dimensionTreeMapModel = new DimensionTreeMapModel();
  this.dimensionTreeMapView = new DimensionTreeMapView(this.dimensionTreeMapModel);
}

DimensionTreeMapController.prototype = {

  handleAppEvent : function(hashParams) {
    this.dimensionTreeMapModel.init(hashParams);
    this.dimensionTreeMapModel.update();
    this.dimensionTreeMapView.render();
  },

}
