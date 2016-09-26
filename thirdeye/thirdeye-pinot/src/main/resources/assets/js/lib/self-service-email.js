// function renderEmailSelfService() {
//   $("#alert").append("<h3>Setup Alerts</h3><table> "
//       + "<tr><td>collection</td><td><select name='collection' id='collection' onchange='fillMetricForCollection()'>"
//       + "</select></td></tr> <tr><td>metric</td>"
//       + "<td><select name='metric' id='metric' onchange='fetchEmailIfPresent()'></select></td></tr>"
//       + "<tr><td>recipients</td><td><input type='text' name='toAddress' id='toAddress' size='80' />"
//       + "</td></tr><tr><td><input type='hidden' id='emailId' name='emailId' value='' />"
//       + "</td><td><input type='submit' name='save' id='submit' onclick='saveEmailConfig()' /></td></tr>"
//       + "</table><div id='email-functions'></div>");
//   //renderDataSets();
// }
//
// function renderDataSets() {
//   getData("/dashboard/data/datasets", "alert").done(function (data) {
//     var select = "<option>Select</option>";
//     for (var i in data) {
//       select += "<option>" + data[i] + "</option>";
//     }
//     $("#collection").append(select);
//   });
// }
//
// function fillMetricForCollection() {
//   clear();
//   var collection = $("#collection").find(':selected').text();
//   if (collection === 'Select') {
//     clear();
//     $("#metric").empty();
//   } else {
//     getData("/dashboard/data/metrics?dataset=" + collection, "alert").done(function (data) {
//       var select = "<option>Select</option>";
//       for (var i in data) {
//         select += "<option>" + data[i] + "</option>";
//       }
//       clear();
//       $("#metric").empty();
//       $("#metric").append(select);
//     });
//   }
// }


function fetchEmailIfPresent() {
  console.log("fetching email details");
  clear();
  var tab = "alert";
  var collection = $("#collection").find(':selected').text();
  var metric = $("#metric").find(':selected').text();
  if (metric != 'Select') {
    getData("/thirdeye/email?collection=" + collection + "&metric=" + metric, tab).done(
        function (data) {
          if (data == undefined || data.length == 0) {
          } else {
            var emailId = data[0].id;
            var recipients = data[0].toAddresses;
            $("#toAddress").val(recipients);
            $("#emailId").val(emailId);
            // get all functions for this metric/collection
            getData("/dashboard/anomaly-function/view?dataset=" + collection + "&metric=" + metric,
                tab).done(function (functionData) {

              var linkedFunctionMap = new Object();
              var unlinkedFunctionMap = new Object();

              // now populate the functions
              var functions = data[0].functions;

              var linkedFunctionNames = "";
              var unlinkedFunctionNames = "";

              if (functions != undefined) {
                for (var i in functions) {
                  if (linkedFunctionNames != "") {
                    linkedFunctionNames += ', ';
                  }
                  linkedFunctionMap[functions[i].id] = functions[i].functionName;
                  linkedFunctionNames += functions[i].functionName
                      + "<a onclick='removeFunctionFromEmail(" + emailId + "," + functions[i].id
                      + ")'>[-]</a>";
                }
              }

              if (functionData != undefined) {
                for (var p in functionData) {
                  if (linkedFunctionMap[functionData[p].id] == undefined) {
                    if (unlinkedFunctionNames != "") {
                      unlinkedFunctionNames += ', ';
                    }
                    unlinkedFunctionMap[functionData[p].id] = functionData[p].functionName;
                    unlinkedFunctionNames += functionData[p].functionName
                        + "<a onclick='addFunctionToEmail(" + emailId + "," + functionData[p].id
                        + ")'>[+]</a>";
                  }
                }
              }
              console.log(linkedFunctionMap);
              console.log(unlinkedFunctionMap);

              if (linkedFunctionNames != "" || unlinkedFunctionNames != "") {
                $("#email-functions").html(
                    "<hr/>Linked Functions : " + linkedFunctionNames + "<br/>Unlinked Functions : "
                    + unlinkedFunctionNames);
              }
            });
          }
        });
  }
}

function clear() {
  $("#toAddress").val("");
  $("#email-functions").html("");
  $("#emailId").val("");
}

function removeFunctionFromEmail(emailId, functionId) {
  console.log("functionRemoved" + functionId);
  submitData("/thirdeye/email/" + emailId + "/delete/" + functionId, "").done(function () {
    fetchEmailIfPresent();
  });
}

function addFunctionToEmail(emailId, functionId) {
  console.log("functionAdded " + functionId);
  submitData("/thirdeye/email/" + emailId + "/add/" + functionId, "").done(function () {
    fetchEmailIfPresent();
  });
}

function saveEmailConfig() {
  var collection = hash.dataset;
  var metric = $("#manage-alerts #selected-metric").attr("value");
  var emailId = $("#emailId").val();
  var toAddress = $("#toAddress").val();

  var payload = "{";
  if (collection != "Select" && metric != 'Select') {
    if (emailId != '') {
      payload += '"id" : ' + emailId + ",";
    }
    payload += '"collection" : "' + collection + '",';
    payload += '"metric" : "' + metric + '",';
    payload += '"toAddresses" : "' + toAddress + '",';
    payload += '"fromAddress" : "thirdeye-dev@linkedin.com",';
    payload += '"cron" : "0 0 0/4 * * ?",';
    payload += '"smtpHost" : "email.corp.linkedin.com",';
    payload += '"active" : true,';
    payload += '"windowDelay" : 0,';
    payload += '"windowDelayUnit" : "MINUTES"';
    payload += "}";
    console.log(payload);
    submitData("/thirdeye/email", payload, "alert").done(function (id) {
      $("#emailId").val(id);
      console.log(id);
      fetchEmailIfPresent();
    });
  } else {
    console.log("missing params");
  }
}

