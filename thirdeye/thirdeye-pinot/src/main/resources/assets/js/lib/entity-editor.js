var entityData;
var editor;

function renderConfigSelector() {
  var html = "<table><tr><td>Select config type</td><td><select name='entityTypeSelector' id='entityTypeSelector' onchange='renderEntitySelector()'></select></td></tr>"
      + "<tr><td>Select entity to edit</td><td><select name='entitySelector' id='entitySelector' onchange='renderEntityEditor()'></select></td></tr>"
      + "</table>" + "<div id='entityDetails' style='width: 700px; height: 500px;'></div>";

  $("#entity-editor-place-holder").html(html);
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
        select += "<option value='" + data[i].id + "'>" + buildNameForEntity(data[i], entityType) + "</option>";
      }
      $("#entitySelector").append(select);
    });
  }
}

function buildNameForEntity(entity, entityType) {
  switch (entityType) {
    case "DASHBOARD_CONFIG":
      return entity.id+" : "+ entity.name;
    case "DATASET_CONFIG":
      return entity.id+" : "+ entity.dataset;
    case "METRIC_CONFIG":
      return entity.id+" : "+ entity.dataset + " : " + entity.name;
    case "ANOMALY_FUNCTION":
      return entity.id+ " : " + entity.functionName + " : " + entity.type;
    case "EMAIL_CONFIGURATION":
      return entity.id + " : " + entity.collection + " : " + entity.metric;
    default:
      console.log("entity type not found : " + entityType);
      return entity.id;
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
