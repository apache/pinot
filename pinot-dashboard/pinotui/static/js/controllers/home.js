pinotApp.controller('homeController', function($scope) {
  $scope.fabrics = [];
  getFabrics(function(fabrics) {
    $scope.$apply(function() {
      $scope.fabrics = fabrics;
      window.location = '#/fabric/'+fabrics.slice(0, 1).toString();
    });
  });
  $scope.pageName = 'Fabric List';
});
