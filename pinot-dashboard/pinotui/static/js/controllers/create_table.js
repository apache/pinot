pinotApp.controller('createTableController', function($scope, $routeParams) {
  $scope.cluster = {
    'name': $routeParams.clusterName,
    'fabric': $routeParams.fabricName,
  }
  $scope.table = {}
  $scope.table.push_frequency = 'days';
  $scope.table.push_type = 'append';

  $scope.table.loadmode = 'heap';
  $scope.table.lazyload = 'false';

  $scope.table.type = 'offline';

  $scope.create = function(data) {
    console.log(data)
  }
});
