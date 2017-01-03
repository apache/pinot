function DimensionTreeMapController() {
  this.dimensionTreeMapModel = new DimensionTreeMapModel();
  this.dimensionTreeMapView = new DimensionTreeMapView(this.dimensionTreeMapModel);
}

DimensionTreeMapController.prototype = {

  handleAppEvent : function(hashParams) {
    console.log("----------------- rendering heatmap with hashParams ---------");
    console.log(hashParams);
    this.dimensionTreeMapModel.init(hashParams);
    this.dimensionTreeMapModel.update();
    this.dimensionTreeMapView.render();
  },
}
