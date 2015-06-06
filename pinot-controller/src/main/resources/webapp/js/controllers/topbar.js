pinotApp.controller('TopBarController', function($scope) {

  $scope.auth = {};
  $scope.auth.enabled = false;
  $scope.auth.authed = false;
  $scope.auth.username = '';

  $scope.fabrics = [];

  getFabrics(function(fabrics) {
    $scope.$apply(function() {
      $scope.fabrics = fabrics;
    });
  });

  $.get(URLUTILS.forAuthInfo(), function(data) {
    validateAjaxCall(data, function(data) {
      if (!data.supported) {
        return;
      }

      $scope.$apply(function() {
        $scope.auth.enabled = true;
        $scope.auth.authed = data.authed;
        if (data.authed) {
          $scope.auth.username = data.username;
        }
      });
    });
  })
});
