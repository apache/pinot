const MAPPING_TYPES = [
  'METRIC',
  'DIMENSION',
  'SERVICE',
  'DATASET',
  'LIXTAG',
  'CUSTOM'
];

var services;
var datasets;

function renderMappingsSelection() {
  var html = "<table style='border: 1px;width: 80%'>"
      + "<tr><td>When showing this:</td><td><select name='entityTypeSelector1' id='entityTypeSelector1' onchange='renderEntityTypeSelector(this.id)'></select><div id='entityTypeSelector1_details'></div></td></tr>"
      + "<tr><td>Also show this:</td><td><select name='entityTypeSelector2' id='entityTypeSelector2' onchange='renderEntityTypeSelector(this.id)'></select><div id='entityTypeSelector2_details'></div></td></tr>"
      + "<tr><td>Priority (0.0 - 1.0)</td><td><input name='entityScore' id='entityScore' value='1.0'/></td></tr>"
      + "</table>"
      + "<input type='submit' onclick='updateEntityMapping()' />"
      + "<hr/><div id='existing_mappings'></div>"
      + "<hr/><table id='existing-mappings-data-table'></table>";
  $("#mappings-place-holder").html(html);

  // dropdown options
  var select = "<option value='select'>Select</option>";
  for (var i in MAPPING_TYPES) {
    select += "<option value='" + MAPPING_TYPES[i] + "'>" + MAPPING_TYPES[i] + "</option>";
  }
  $("#entityTypeSelector1").html(select);
  $("#entityTypeSelector2").html(select);

  // pre load the services
  getData("/external/services/all", "admin").done(function (data) {
    services = data;
  });

  // pre load the datasets
  getData("/data/datasets", "admin").done(function (data) {
    datasets = data;
  });
}

function renderEntityTypeSelector(divId) {
  var entityType = $("#" + divId).find(':selected').val();
  $("#" + divId + "_details").html(getEntitySpecificSelectionOptions(divId, entityType));

  var entitySelect = $("#" + divId + "_" + entityType);
  var entityUrn = $("#" + divId + "_" + entityType + "_urn");

  if (entityType === 'METRIC') {
    entitySelect.select2({
      ajax: {
        url: "/data/autocomplete/metric", delay: 250, data: function (params) {
          var query = {
            name: params.term
          };
          // Query parameters will be ?name=[term]&page=[page]
          return query;
        }, processResults: function (data) {
          var results = [];
          $.each(data, function (index, item) {
            results.push({
              id: item.id, text: item.alias, name: item.name
            });
          });
          return {
            results: results
          };
        }
      }
    });
    entitySelect.on("change", function (e) {
      const $selectedElement = $(e.currentTarget);
      const selectedData = $selectedElement.select2('data');
      var metricId = "";
      if (selectedData.length) {
        const {id, text, name} = selectedData[0];
        metricId = id;
        metricAlias = text;
        metricName = name;
      }
      entityUrn.val(getUrnForPrefixEntityType(entityType) + metricId);
    });

  } else if (entityType === 'SERVICE') {
    entitySelect.select2({
      placeholder: "select a service",
      data: services
    });
    entitySelect.on("change", function (e) {
      const $selectedElement = $(e.currentTarget);
      const serviceArr = $selectedElement.select2('data');
      if (serviceArr.length) {
        entityUrn.val(getUrnForPrefixEntityType(entityType) + serviceArr[0]['id']);
      }
    });

  } else if (entityType === 'DATASET') {
    entitySelect.select2({
      placeholder: "select a dataset",
      data: datasets
    });
    entitySelect.on("change", function (e) {
      const $selectedElement = $(e.currentTarget);
      const datasetArr = $selectedElement.select2('data');
      if (datasetArr.length) {
        entityUrn.val(getUrnForPrefixEntityType(entityType) + datasetArr[0]['id']);
      }
    });

  } else {
    entityUrn.val(getUrnForPrefixEntityType(entityType));
  }
}

function getUrnForPrefixEntityType(entityType) {
  switch (entityType) {
    case "METRIC":
      return "thirdeye:metric:";
    case "SERVICE":
      return "thirdeye:service:";
    case "DIMENSION":
      return "thirdeye:dimension:";
    case "LIXTAG":
      return "thirdeye:lixtag:";
    case "DATASET":
      return "thirdeye:dataset:";
    default:
      return "thirdeye:";
  }
}

function getEntitySpecificSelectionOptions(baseId, entityType) {
  var html = "";

  var suffixHtml = "";
  var onChange = "";
  if (baseId == "entityTypeSelector1") {
    suffixHtml = "<a href='#' onclick='fetchAndDisplayMappings(\"" + entityType
        + "\")'> refresh</a>";
    onChange = " onchange='fetchAndDisplayMappings(\"" + entityType + "\")'";
  }

  switch (entityType) {
    case "METRIC":
    case "SERVICE":
    case "DATASET":
      html += "<select style='width:100%' id='" + baseId + "_" + entityType + "'></select><div id='"
          + baseId + "_final_urn' ></div>";
      break;
    default:
      // do nothing
  }
  html += "<input style='width:100%' type='text' placeholder='provide urn' id='" + baseId + "_"
      + entityType + "_urn'" + onChange + "/>" + suffixHtml;
  return html;
}

function updateEntityMapping() {
  const entityType1 = $("#entityTypeSelector1").find(':selected').val();
  const entityType2 = $("#entityTypeSelector2").find(':selected').val();
  const fromUrn = $("#entityTypeSelector1_" + entityType1 + "_urn").val();
  const toUrn = $("#entityTypeSelector2_" + entityType2 + "_urn").val();
  const score = $("#entityScore").val();

  console.log(entityType1, entityType2, fromUrn, toUrn, score);

  const payload = '{ "fromURN": "' + fromUrn + '", "toURN": "' + toUrn + '", "mappingType": "'
      + entityType1 + "_TO_" + entityType2 + '", "score": "' + score + '"}';

  if (entityType1 == 'select' || entityType2 == 'select' || fromUrn == '' || toUrn == '' || score == '') {
    return;
  }

  if (fromUrn.includes(" ") || fromUrn.includes(",") || fromUrn.includes(";")
      || toUrn.includes(" ") || toUrn.includes(",") || toUrn.includes(";")) {
    alert("Cannot contain separators except ':' (colon)");
    return;
  }

  submitData("/entityMapping/create", payload, "admin", "html").success(function (data) {
    console.log("Adding mapping : ");
    fetchAndDisplayMappings(entityType1);
  });
}

function fetchAndDisplayMappings(entityType) {
  console.log("fetching and displaying data for " + entityType);
  var fromUrn = $("#entityTypeSelector1_" + entityType + "_urn").val();
  if (fromUrn == '') {
    return;
  }

  getData("/entityMapping/view/fromURN/" + fromUrn, "admin").success(function (urns) {
    const urn2entity = {};
    for (var i = 0; i < urns.length; i++) {
      const u = urns[i];
      urn2entity[u.toURN] = u;
    }

    const urnsString = urns.map(function (x) {
      return (x.toURN)
    }).join(",");

    if ($.fn.dataTable.isDataTable( '#existing-mappings-data-table' )) {
      // existing table
      $('#existing-mappings-data-table').DataTable().destroy();
      $('#existing-mappings-data-table').empty();
    }

    // new table
    $('#existing-mappings-data-table').DataTable({
      ajax: {
        url: '/rootcause/raw?framework=identity&urns=' + urnsString, dataSrc: ''
      }, columns: [{
        title: 'Type', data: null, render: function (data, type, row) {
          return ("<a href=\"" + row.link + "\" target=\"_blank\">" + row.type + "</a>");
        }
      }, {
        title: 'toURN', data: 'urn'
      }, {
        title: 'Label', data: 'label'
      }, {
        title: 'Priority', data: null, render: function (data, type, row) {
          if (row.urn in urn2entity) {
            return (urn2entity[row.urn].score);
          } else {
            return ('-')
          }
        }
      }, {
        title: '', data: null, render: function (data, type, row) {
          if (row.urn in urn2entity) {
            return ("<a href=\"#\" onclick=\"deleteMapping(\'" + urn2entity[row.urn].id + "\')\">delete</a>");
          } else {
            return ('-')
          }
        }
      }], order: [[3, 'desc'], [2, 'asc'], [1, 'asc']], iDisplayLength: 50, retrieve: true
    });

  });
}

function deleteMapping(id) {
  deleteData("/entityMapping/delete/" + id, "", "mapping", "html").success(function (data) {
    console.log("deleted mapping with id : " + id);
    const entityType1 = $("#entityTypeSelector1").find(':selected').val();
    fetchAndDisplayMappings(entityType1);
  });
}
