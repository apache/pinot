pinotApp.controller('consoleController', function($scope) {
  $scope.fabrics = []
  $scope.pageName = 'Query Console';
  $scope.queryConsole = true;

  var editor = CodeMirror.fromTextArea(document.getElementById('cm_prompt'), {
    lineWrapping: true,
    lineNumbers: true,
    mode: "sql",
    theme: "elegant"
  });

  var result = CodeMirror.fromTextArea(document.getElementById('cm_result'), {
    lineWrapping: true,
    lineNumbers: true,
    mode: "javascript",
    theme: "elegant",
    readOnly: true,
    smartIndent: true,
  });

  var fabric_menu = $('#pick_fabric');

  getFabrics(function(fabrics) {
    $scope.$apply(function() {
      $scope.fabrics = fabrics;
    });
  });

  $('#run_code').click(function() {
    var pql = editor.getValue().trim();
    var fabric = fabric_menu.val();

    if (fabric == '') {
      alert('pick a fabric');
      return;
    }

    if (pql == '') {
      alert('type a query');
      return;
    }

    $.get(URLUTILS.forPql(fabric, pql), function(data) {
      validateAjaxCall(data, function(data) {
        result.setValue(JSON.stringify(data.result));
      })
    });
  });
});
