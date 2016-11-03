function listIngraphDashboardConfigs() {
  $('#ingraph-dashboard-config-place-holder').jtable({
    title : 'Ingraph Dashboard Configs' ,
    paging : true, // Enable paging
    sorting : true, // Enable sorting
    ajaxSettings : {
      type : 'GET',
      dataType : 'json'
    },
    actions : {
      listAction : '/thirdeye-admin/ingraph-dashboard-config/list',
      createAction : '/thirdeye-admin/ingraph-dashboard-config/create',
      updateAction : '/thirdeye-admin/ingraph-dashboard-config/update',
      deleteAction : '/thirdeye-admin/ingraph-dashboard-config/delete'
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
        title : 'DashboardName'
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
          if (data.record && data.record.bootstrapStartTime)
            return moment(data.record.bootstrapStartTime).format('YYYY-MM-DD');
          return '';
        }
      },
      endTime : {
        title : 'End Time',
        type : 'date',
        display : function(data) {
          if (data.record && data.record.bootstrapEndTime)
            return moment(data.record.bootstrapStartTime).format('YYYY-MM-DD');
          return '';
        }
      },
      fetchIntervalPeriod : {
        title : 'Interval Period'
      },
      fabrics : {
        title : 'Fabrics'
      },
      granularitySize : {
        title : 'Granularity Size'
      },
      granularityUnit : {
        title : 'Granularity Unit',
        options : [ 'MINUTES', 'HOURS', 'DAYS' ]
      },
      mergeNumAvroRecords : {
        title : 'Num Avro Records',
        visibility: 'hidden'
      }
    }
  });
  $("#ingraph-dashboard-config-place-holder").jtable('load');
}