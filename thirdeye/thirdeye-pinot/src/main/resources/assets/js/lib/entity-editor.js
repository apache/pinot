var entityData;
var editor;

function renderConfigSelector() {
  var html = "<table><tr><td>Select config type</td><td><select name='entityTypeSelector' id='entityTypeSelector' onchange='renderEntitySelector()'></select></td></tr>"
      + "<tr><td>Select entity to edit</td><td><select name='entitySelector' id='entitySelector' onchange='renderEntityEditor()'></select></td></tr>"
      + "</table>" + "<div id='entityEditorDiv'>"
      + "<table><tr><td><div id='entityDetails' style='width: 700px; height: 500px;'></div></td>"
      + "<td><div id='entityDetailsTextArea' style='width: 700px; height: 500px;'></div></td></tr></table>"
      + "</div> <div id='entityEditorBottom'></div>";

  $("#entity-editor-place-holder").html(html);
  getData("/thirdeye/entity", "admin").done(function (data) {
    var select = "<option value='select'>Select</option>";
    for (var i in data) {
      select += "<option value='" + data[i] + "'>" + data[i] + "</option>";
    }
    $("#entityTypeSelector").html(select);
  });
}

function renderEntitySelector() {
  var entityType = $("#entityTypeSelector").find(':selected').val();

  clear();

  if (entityType != 'select') {
    getData("/thirdeye/entity/" + entityType, "admin").done(function (data) {
      entityData = data;
      var select = "<option value='select'>Select</option>";
      select += "<option> Create New </option>";
      for (var i in data) {
        select += "<option value='" + data[i].id + "'>" + buildNameForEntity(data[i], entityType)
            + "</option>";
      }
      $("#entitySelector").html(select);
    });
  }
}

function buildNameForEntity(entity, entityType) {
  switch (entityType) {
    case "DASHBOARD_CONFIG":
      return entity.id + " : " + entity.name;
    case "DATASET_CONFIG":
      return entity.id + " : " + entity.dataset;
    case "METRIC_CONFIG":
      return entity.id + " : " + entity.dataset + " : " + entity.name;
    case "ANOMALY_FUNCTION":
      return entity.id + " : " + entity.functionName + " : " + entity.type;
    case "OVERRIDE_CONFIG":
      var startTime = new Date(entity.startTime);
      var endTime = new Date(entity.endTime);
      return entity.id + " : " + entity.targetEntity + " : "+ startTime.toString() + " -- "
             + endTime.toString();
    case "ALERT_CONFIG" :
      return entity.id + " : " + entity.name;
    case "APPLICATION" :
      return entity.id + " : " + entity.application;
    case "ENTITY_MAPPING" :
        console.log(entity)
      return entity.id + " : " + entity.fromURN + "--->" + entity.toURN;
    default:
      console.log("entity type not found : " + entityType);
      return entity.id;
  }
}

function renderEntityEditor() {

  clear();

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

  var submitHtml = "<p/><input type='submit' name='submit' onclick='updateObject()' />";
  $("#entityEditorBottom").html(submitHtml);

  $("#entityDetailsTextArea").html("<textarea rows='20' cols='50' id='jsonTextArea'>" + JSON.stringify(editor.get(), null, 4) + "</textarea>");
  $("#entityDetailsTextArea").append("<p/><input type='button' value='load to editor' onclick='loadToEditor()' />");
}

function updateObject() {
  var entityType = $("#entityTypeSelector").find(':selected').val();
  var jsonVal = JSON.stringify(editor.get());
  submitData("/thirdeye/entity?entityType=" + entityType, jsonVal, "admin").done(function (data) {
    console.log(data);
    renderEntitySelector();
  })
}

function loadToEditor() {
  editor.set(JSON.parse($("#jsonTextArea").val()));
}

function clear() {
  $("#entityDetails").empty();
  $("#entityDetailsTextArea").empty();
  $("#entityEditorBottom").empty();
}
