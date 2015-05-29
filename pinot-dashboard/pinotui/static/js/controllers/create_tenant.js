pinotApp.controller('createTenantController', function($scope, $routeParams) {
  var fabric = $routeParams.fabricName;
  $scope.fabric = fabric;
  $scope.tenant = {}
  $scope.tenant.type = 'server';

  $scope.create = function(data) {
    console.log(data)
  }
});
