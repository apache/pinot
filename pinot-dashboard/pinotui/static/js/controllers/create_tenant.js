pinotApp.controller('createTenantController', function($scope, $routeParams) {
  var fabric = $routeParams.fabricName;
  $scope.fabric = fabric;
  $scope.tenant = {}
  $scope.tenant.type = 'server';

  $scope.create = function(form) {
    var json = JSON.stringify(form);
    $.ajax(URLUTILS.forCreateTenant(fabric), {
      data: json,
      contentType: 'application/json',
      type: 'POST',
      success:  function(data) {
        validateAjaxCall(data, function() {
          alert('Post request successful');
          window.location = '#/fabric/'+fabric;
        });
      }
    });
  }
});
