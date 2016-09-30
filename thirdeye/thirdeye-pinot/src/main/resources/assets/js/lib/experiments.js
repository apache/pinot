var entityData;
var editor;

function renderAdminPage() {
  $('#admin').append("<h2>Welcome Thirdeye Admin!</h2>"
      + "<table style='vertical-align: top'><tr style='vertical-align: text-top;'>"
      + "<td id='admin-tab-area'><ul style='display: block;list-style-type: none;margin: 2em;align-content: flex-start;'>"
      + "<li><a onclick='renderConfigSelector()'>View & Edit Configs</a></li>"
      + "<li><a>Test Anomaly Functions</a></li></ul></td>" + "<td id='admin-display-area'></td>"
      + "</tr></table>");
  renderConfigSelector();
}

function renderConfigSelector() {
  var html = "<table><tr><td>Select config type</td><td><select name='entityTypeSelector' id='entityTypeSelector' onchange='renderEntitySelector()'></select></td></tr>"
      + "<tr><td>Select entity to edit</td><td><select name='entitySelector' id='entitySelector' onchange='renderEntityEditor()'></select></td></tr>"
      + "</table>" + "<div id='entityDetails' style='width: 700px; height: 600px;'></div>";

  $("#admin-display-area").html(html);
  getData("/thirdeye/entity", "admin").done(function (data) {
    var select = "<option value='select'>Select</option>";
    for (var i in data) {
      select += "<option value='" + data[i] + "'>" + data[i] + "</option>";
    }
    $("#entityTypeSelector").append(select);
  });
}

function renderEntitySelector() {
  var entityType = $("#entityTypeSelector").find(':selected').val();
  $("#entitySelector").empty();
  $("#entityDetails").empty();
  if (entityType != 'select') {
    getData("/thirdeye/entity/" + entityType, "admin").done(function (data) {
      entityData = data;
      var select = "<option value='select'>Select</option>";
      for (var i in data) {
        select += "<option value='" + data[i].id + "'>" + data[i].id + "</option>";
      }
      $("#entitySelector").append(select);
    });
  }
}

function renderEntityEditor() {
  $("#entityDetails").empty();
  var element = document.getElementById("entityDetails");
  editor = new JSONEditor(element, {schema: {type: "object"}});
  var entity = "";
  var entityId = $("#entitySelector").find(':selected').val();
  for (var i in entityData) {
    if (entityData[i].id == entityId) {
      entity = entityData[i];
    }
  }
  editor.set(entity);
  var submitHtml = "<input type='submit' name='submit' onclick='updateObject()' />";
  $("#entityDetails").append(submitHtml);
}

function updateObject() {
  var entityType = $("#entityTypeSelector").find(':selected').val();
  var jsonVal = JSON.stringify(editor.get());
  submitData("/thirdeye/entity?entityType=" + entityType, jsonVal, "admin").done(function (data) {
    console.log(data);
  })
}

function renderFunctionAnalyze() {
  $('#admin').append("<h3>Analyze Jobs</h3><textarea id='function'></textarea>"
      + "<input type='submit' onclick='testAnomalyJob()' />");
}

function testAnomalyJob() {
// TODO:
}
