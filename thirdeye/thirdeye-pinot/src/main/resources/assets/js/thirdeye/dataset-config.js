function listDatasetConfigs() {
  $('#dataset-config-place-holder').jtable({
    title : 'Dataset Configs' ,
    paging : true, // Enable paging
    sorting : true, // Enable sorting
    ajaxSettings : {
      type : 'GET',
      dataType : 'json'
    },
    actions : {
      listAction : '/thirdeye-admin/dataset-config/list',
      createAction : '/thirdeye-admin/dataset-config/create',
      updateAction : '/thirdeye-admin/dataset-config/update',
      deleteAction : '/thirdeye-admin/dataset-config/delete'
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
      dataset : {
        title : 'Dataset'
      },
      dimensions : {
        title : 'dimensions',
        width:"20%",
        display: function(data){
          return "\"" + data.record.dimensions + "\"";
             // return "a,b,c"
        }
      },
      dimensionsHaveNoPreAggregation : {
        title : 'dimensionsHaveNoPreAggregation',
        visibility : 'hidden'
      },
      active : {
        title : 'Active',
        defaultValue : false,
        options : [ false, true ]
      },
      additive : {
        title : 'Additive',
        defaultValue : true,
        options : [ false, true ]
      },
      nonAdditiveBucketSize : {
        title : 'nonAdditiveBucketSize',
        visibility: 'hidden'
      },
      nonAdditiveBucketUnit : {
        title : 'nonAdditiveBucketUnit',
        visibility: 'hidden'
      },
      preAggregatedKeyword : {
        title : 'preAggregatedKeyword',
        visibility: 'hidden'
      },
      timeColumn : {
        title : 'timeColumn'
      },
      timeDuration : {
        title : 'timeDuration'
      },
      timeFormat : {
        title : 'timeFormat'
      },
      timeUnit : {
        title : 'timeUnit'
      },
      timezone : {
        title : 'timezone',
        visibility: 'hidden'
      },

    }
  });
  $("#dataset-config-place-holder" ).jtable('load');
}