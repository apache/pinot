function showDatasetSelection() {

  $
      .ajax({
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
          console.log(result_ingraph_metric_config_section)
          $("#ingraph-metric-config-place-holder").append(
              result_ingraph_metric_config_section);
          $("#ingraph-metric-config-place-holder").show();
          // updateIngraphMetricConfigTable()
          $('#ingraph-dataset-selector').change(function() {
            updateIngraphMetricConfigTable(this.value)
          })
        },
        error : function() {
          alert("Failed to load genders");
        }
      })

}

function updateIngraphMetricConfigTable(dataset) {
  //hide all jtables
  $('.IngraphMetricContainer').hide();
  //show the jtable for this dataset
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
      listAction : '/ingraph-metric-config/list?dataset=' + dataset,
      createAction : '/ingraph-metric-config/create?dataset=' + dataset,
      updateAction : '/ingraph-metric-config/update?dataset=' + dataset,
      deleteAction : '/ingraph-metric-config/delete?dataset=' + dataset
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
      metric : {
        title : 'Metric  Name'
      },
      metricAlias : {
        title : 'Metric Alias'
      },
      metricDataType : {
        title : 'Metric Type',
        options : [ 'INT', 'FLOAT' ]
      },
      metricSourceType : {
        title : 'Source Type',
        options : [ 'COUNTER', 'GAUGE' ]
      },
      bootstrap : {
        title : 'Bootstrap',
        defaultValue : false,
        options : [ false, true ]
      },
      startTime : {
        title : 'Start Time',
        type : 'date',
        display : function(data) {
          if (data.record && data.record.endTimeInMs)
            return moment(data.record.startTimeInMs).format('YYYY-MM-DD');
          return '';
        }
      },
      endTime : {
        title : 'End Time',
        type : 'date',
        display : function(data) {
          if (data.record && data.record.startTimeInMs)
            return moment(data.record.endTimeInMs).format('YYYY-MM-DD');
          return '';
        }
      }
    }
  });
  $("#IngraphMetricContainer-"+dataset).jtable('load');
}