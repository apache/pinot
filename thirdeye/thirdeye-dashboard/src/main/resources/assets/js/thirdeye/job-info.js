function listJobs() {

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
      var result_job_info_section = job_info_template_compiled(datasets);
      $("#job-info-place-holder").html(result_job_info_section);
      $("#job-info-place-holder").show();
      $('#job-dataset-selector').change(function() {
        updateJobInfoTable(this.value)
      })
    },
    error : function() {
      alert("Failed to load dataset list");
    }
  })
}

function updateJobInfoTable(dataset) {
  // hide all jtables
  $('.JobInfoContainer').hide();
  // show the jtable for this dataset
  $('#JobInfoContainer-' + dataset).show();
  listEndPoint = '/thirdeye-admin/job-info/listJobsForDataset?dataset=' + dataset
  if(dataset == "MOST-RECENT"){
    listEndPoint = '/thirdeye-admin/job-info/listRecentJobs'
  }
  $('#JobInfoContainer-' + dataset).jtable({
    title : 'Job Info for ' + dataset,
    paging : true, // Enable paging
    sorting : true, // Enable sorting
    ajaxSettings : {
      type : 'GET',
      dataType : 'json'
    },
    actions : {
      listAction : listEndPoint
    },
    fields : {
      id : {
        title : 'Job Id',
        key : true,
        list : true
      },
      jobName : {
        title : 'Job Name'
      },
      status : {
        title : 'Status'
      },
      scheduleStartTime : {
        title : 'scheduleStartTime',
        type: 'datetime'
      },
      scheduleEndTime : {
        title : 'scheduleEndTime',
        type: 'datetime'
      },
      monitoringWindowStartTime : {
        title : 'monitoringWindowStartTime',
        type: 'datetime'
      },
      monitoringWindowEndTime : {
        title : 'monitoringWindowStartTime',
        type: 'datetime'
      },
    }
  });
  $("#JobInfoContainer-" + dataset).jtable('load');
}