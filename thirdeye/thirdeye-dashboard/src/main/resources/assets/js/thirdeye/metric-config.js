function showMetricDatasetSelection() {

  $.ajax({
    type : "GET",
    url : "/data/datasets",
    data : encodeURIComponent("{}"),
    contentType : "application/json; charset=utf-8",
    dataType : "json",
    success : function(data) {
      var datasets = {
        "datasets" : data
      }
      console.log(datasets)
      var result_metric_config_section = metric_config_template_compiled(datasets);
      $("#metric-config-place-holder").html(result_metric_config_section);
      $("#metric-config-place-holder").show();
      $('#metric-dataset-selector').change(function() {
        metrics = getMetricSet(this.value);
      })
    },
    error : function() {
      alert("Failed to load dataset list");
    }
  })
}

function getMetricSet(dataset){
  $.ajax({
    type : "GET",
    url : "/thirdeye-admin/metric-config/metrics?dataset=" + dataset,
    data : encodeURIComponent("[]"),
    contentType : "application/json; charset=utf-8",
    dataType : "json",
    success : function(data) {
      console.log(data["Records"])
      updateMetricConfigTable(dataset, data["Records"])
    },
    error : function() {
      alert("Failed to load dataset list");
      return "[]";
    }
  })
}
function updateMetricConfigTable(dataset, metrics) {
  console.log("metrics")
  console.log(metrics)
  // hide all jtables
  $('.MetricConfigContainer').hide();
  // show the jtable for this dataset
  $('#MetricConfigContainer-' + dataset).show();
  $('#MetricConfigContainer-' + dataset).jtable({
    title : 'Metrics for ' + dataset,
    paging : true, // Enable paging
    sorting : true, // Enable sorting
    ajaxSettings : {
      type : 'GET',
      dataType : 'json'
    },
    actions : {
      listAction : '/thirdeye-admin/metric-config/list?dataset=' + dataset,
      createAction : '/thirdeye-admin/metric-config/create?dataset=' + dataset,
      updateAction : '/thirdeye-admin/metric-config/update?dataset=' + dataset,
      deleteAction : '/thirdeye-admin/metric-config/delete?dataset=' + dataset
    },
    formCreated : function(event, data) {
      data.form.css('width', '500px');
      data.form.find('input').css('width', '100%');
    },
    formSubmitting : function(event, data) {
      return true;
    },
    fields : {
      id : {
        title : 'Id',
        key : true,
        list : true
      },
      name : {
        title : 'Name'
      },
      alias : {
        title : 'Alias'
      },
      datatype : {
        title : 'Data Type',
        options : [ 'INT', 'LONG','FLOAT', 'DOUBLE' ]
      },
      active : {
        title : 'Active',
        defaultValue : false,
        options : [ false, true ]
      },
      metricAsDimension : {
        title : 'MetricAsDimension',
        defaultValue : false,
        options : [ false, true ]
      },
      derived : {
        title : 'Derived',
        defaultValue : false,
        options : [ false, true ]
      },
      derivedFunctionType : {
        title : 'FunctionType',
        defaultValue : "---",
        options : [ "PERCENT", "RATIO" ]
      },
      numerator : {
        title : 'Numerator',
        defaultValue : "---",
        options : metrics
      },
      denominator : {
        title : 'Denominator',
        defaultValue : "---",
        options : metrics
      },
      derivedMetricExpression : {
        title : 'Expression'
      },
      inverseMetric : {
        title : 'Inverse Metric',
        visibility: 'hidden'
      },
      cellSizeExpression : {
        title : 'CellSize Expression',
        visibility: 'hidden'
      },
      rollupThreshold : {
        title : 'Rollup Threshold',
        defaultValue : '0.01',
        visibility: 'hidden'
      }
    }
  });
  $("#MetricConfigContainer-" + dataset).jtable('load');
}