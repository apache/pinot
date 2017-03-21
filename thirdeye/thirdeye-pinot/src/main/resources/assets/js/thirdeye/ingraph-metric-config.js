function showIngraphDatasetSelection() {

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
      var result_ingraph_metric_config_section = ingraph_metric_config_template_compiled(datasets);
      $("#ingraph-metric-config-place-holder").html(result_ingraph_metric_config_section);
      $("#ingraph-metric-config-place-holder").show();
      $('#ingraph-dataset-selector').change(function() {
        updateIngraphMetricConfigTable(this.value)
      })
    },
    error : function() {
      alert("Failed to load dataset list");
    }
  })

}

function updateIngraphMetricConfigTable(dataset) {
  // hide all jtables
  $('.IngraphMetricContainer').hide();
  // show the jtable for this dataset
  $('#IngraphMetricContainer-' + dataset).show();
  $('#IngraphMetricContainer-' + dataset).jtable({
    title : 'Ingraph Metrics for ' + dataset,
    paging : true, // Enable paging
    sorting : true, // Enable sorting
    ajaxSettings : {
      type : 'GET',
      dataType : 'json'
    },
    actions : {
      listAction : '/thirdeye-admin/ingraph-metric-config/list?dashboardName=' + dataset,
      createAction : '/thirdeye-admin/ingraph-metric-config/create?dashboardName=' + dataset,
      updateAction : '/thirdeye-admin/ingraph-metric-config/update?dashboardName=' + dataset,
      deleteAction : '/thirdeye-admin/ingraph-metric-config/delete?dashboardName=' + dataset
    },
    formCreated : function(event, data) {
      data.form.css('width', '600px');
      data.form.find('input').css('width', '100%');
    },
    formSubmitting : function(event, data) {
      return true;
    },
    fields : {
      id : {
        title : 'Metric Id',
        key : true,
        list : true
      },
      container : {
        title : 'Container'
      },
      dashboardName : {
        title : 'Dashboard'
      },
      metricName : {
        title : 'Metric  Name'
      },
      rrdName : {
        title : 'RRD'
      },
      metricDataType : {
        title : 'Metric Type',
        options : [ 'INT', 'FLOAT' ]
      },
      metricSourceType : {
        title : 'Source Type',
        options : [ 'COUNTER', 'GAUGE' ]
      }
    }
  });
  $("#IngraphMetricContainer-" + dataset).jtable('load');
}
