pinotApp.controller('TopBarController', function($scope) {
  $scope.fabrics = [];
  getFabrics(function(fabrics) {
    $scope.$apply(function() {
      $scope.fabrics = fabrics;
    });
  });
});

