function fetchEmailIfPresent() {

  clear();
  tab = "anomalies";
  var dataset = hash.dataset;
  var metric = $("#configure-emails-form-table #selected-metric").attr("value");

  if (metric) {
      getDataCustomCallback("/thirdeye/email?collection=" + dataset + "&metric=" + metric, tab)
      .done(function (data) {
          if ( data && data.length > 0){
              var emailId = data[0].id;
              var recipients = data[0].toAddresses;
              $("#to-address").val(recipients);
              $("#email-id").val(emailId);
              // get all functions for this metric/dataset
              getDataCustomCallback("/dashboard/anomaly-function?dataset=" + dataset + "&metric=" + metric, tab)
              .done(function (functionData) {

              var linkedFunctionMap = new Object();
              var unlinkedFunctionMap = new Object();

              // now populate the functions
              var functions = data[0].functions;

              var linkedFunctionNames = "";
              var unlinkedFunctionNames = "";

              if (functions) {
                  for (var k in functions) {
                      linkedFunctionMap[functions[k].id] = functions[k].functionName;
                      linkedFunctionNames += "<li class='remove-linked-function uk-button  uk-display-block' data-email-id='" + emailId + "'  data-fn-id='"+ functions[k].id + "' style='position:relative;'><a href='#'>" + functions[k].functionName + " <i style='position:absolute;top:5px; right:5px;' class='uk-icon-arrow-circle-right'></i></a></li>";
                  }
              }

              if (functionData) {
                  for (var p in functionData) {
                      if (typeof linkedFunctionMap[functionData[p].id] == "undefined") {
                         unlinkedFunctionMap[functionData[p].id] = functionData[p].functionName;
                         unlinkedFunctionNames +=  "<li class='add-to-linked-function uk-button uk-display-block' data-email-id='" + emailId + "'  data-fn-id='"+ functionData[p].id +"' style='position:relative;'><a href='#'><i style='position:absolute; top:5px; left:5px;' class='uk-icon-arrow-circle-left'></i>" + functionData[p].functionName + "</a></li>"
                      }
                  }
              }

              if (linkedFunctionNames != "" || unlinkedFunctionNames != "") {
                $("#linked-functions").html(linkedFunctionNames);
                $("#unlinked-functions").html(unlinkedFunctionNames);
              }
            });
          }
        });
  }
}

function clear() {
  $("#to-address").val("");
  $("#linked-functions").html("");
  $("#unlinked-functions").html("");
  $("#email-id").val("");
}

function removeFunctionFromEmail(target) {
    var $target = $(target);
    var emailId =  $target.attr("data-email-id");
    var functionId = $target.attr("data-fn-id");

  submitData("/thirdeye/email/" + emailId + "/delete/" + functionId, "").done(function () {
    fetchEmailIfPresent();
  });
}

function addFunctionToEmail(target) {

    var $target = $(target);
    var emailId =  $target.attr("data-email-id");
    var functionId = $target.attr("data-fn-id");

  submitData("/thirdeye/email/" + emailId + "/add/" + functionId, "").done(function () {
    fetchEmailIfPresent();
  });
}

function saveEmailConfig() {

    /* Validate form */
    var errorMessage = $("#manage-alerts-error p");
    var errorAlert = $("#manage-alerts-error");

    var emailListStr = $("#to-address").val();
    //Change ';' to ','
    emailListStr =   emailListStr.replace(/\;/g, ',');
    //Remove trailing comma
    emailListStr =   emailListStr.replace(/\,$/, '');
    var emailListAry = emailListStr.split(',');
    var emailListToPost = [];
    for (var n = 0; n < emailListAry.length; n++) {
        var trim_email = $.trim(emailListAry[n]);


        var validRegExp = /^\w+((-\w+)|(\.\w+))*\@[A-Za-z0-9]+((\.|-)[A-Za-z0-9]+)*\.[A-Za-z0-9]+$/;

        if (trim_email.search(validRegExp) == -1) {
            errorMessage.html('Address list contains an invalid value: "' + trim_email +'".');
            errorAlert.attr("data-error-source", "to-address");
            errorAlert.fadeIn(100);
            return
        }
        emailListToPost.push(trim_email);

    }
    emailListStr = emailListToPost.join(",")

    var dataset = hash.dataset;
    var metric = $("#manage-alerts #selected-metric").attr("value");
    var emailId = $("#email-id").val();
    var toAddress = emailListStr;

    var payload = "{";
    if (dataset && metric) {
        if (emailId != '') {
          payload += '"id" : ' + emailId + ",";
        }
        payload += '"collection" : "' + dataset + '",';
        payload += '"metric" : "' + metric + '",';
        payload += '"toAddresses" : "' + toAddress + '",';
        payload += '"fromAddress" : "thirdeye-dev@linkedin.com",';
        payload += '"cron" : "0 30 0/4 * * ?",'; // run once every 4 hours starting 00:30
        payload += '"smtpHost" : "email.corp.linkedin.com",';
        payload += '"active" : true,';
        payload += '"windowSize" : 7,';
        payload += '"windowUnit" : "DAYS",';
        payload += '"windowDelay" : 0,';
        payload += '"windowDelayUnit" : "MINUTES"';
        payload += "}";
        submitData("/thirdeye/email", payload, "alert").done(function (id) {
          $("#manage-alerts-success").fadeIn();
          $("#email-id").val(id);
          fetchEmailIfPresent();
        });
  } else {
        errorMessage.html('Please select dataset and metric.');
        errorAlert.attr("data-error-source", "params");
        errorAlert.fadeIn(100);
        return
  }
}

