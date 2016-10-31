function showMetricDatasetSelection() {

  $.ajax({
    type : "GET",
    url : "/dashboard/data/datasets",
    data : "{}",
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
        updateMetricConfigTable(this.value)
      })
    },
    error : function() {
      alert("Failed to load dataset list");
    }
  })

}

function updateMetricConfigTable(dataset) {
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
      metricDataType : {
        title : 'Data Type',
        options : [ 'INT', 'LONG','FLOAT', 'DOUBLE' ]
      },
      active : {
        title : 'Active',
        defaultValue : false,
        options : [ false, true ]
      },
      derived : {
        title : 'Derived',
        defaultValue : false,
        options : [ false, true ]
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
      }
    }
  });
  $("#MetricConfigContainer-" + dataset).jtable('load');
}