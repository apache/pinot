pinotApp.controller('createTableController', function($scope, $routeParams) {
  var fabric = $routeParams.fabricName;
  var cluster = $routeParams.clusterName;
  $scope.cluster = {
    'name': cluster,
    'fabric': fabric,
  }
  $scope.table = {}
  $scope.table.push_frequency = 'days';
  $scope.table.push_type = 'append';

  $scope.table.loadmode = 'heap';
  $scope.table.lazyload = 'false';

  $scope.table.type = 'offline';

  $scope.create = function(form) {
    var json = JSON.stringify(form);
    $.ajax(URLUTILS.forCreateTable(fabric, cluster), {
      data: json,
      contentType: 'application/json',
      type: 'POST',
      success: function(data) {
      validateAjaxCall(data, function() {
        alert('Post request successful');
        window.location = '#/resource/'+fabric+'/'+cluster;
      });
    }});
  }
});
