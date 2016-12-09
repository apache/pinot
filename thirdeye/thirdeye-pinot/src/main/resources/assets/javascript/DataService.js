function DataService() {

}

DataService.prototype = {

    getData: function(url, params)  {
      console.log("request url:", url)

      return $.ajax({
          url: url,
          type: 'get',
          dataType: 'json',
          statusCode: {
              404: function () {
                  $("#" + tab + "-chart-area-error").empty();
                  var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' });
                  var closeBtn = $('<i></i>', { class: 'close-parent uk-icon-close' });
                  warning.append($('<p></p>', { html: 'No data available. (Error code: 404)' }));
                  $("#" + tab + "-chart-area-error").append(closeBtn);
                  $("#" + tab + "-chart-area-error").append(warning);
                  $("#" + tab + "-chart-area-error").fadeIn(100);
                  return
              },
              500: function () {
                  $("#" + tab + "-chart-area-error").empty()
                  var error = $('<div></div>', { class: 'uk-alert uk-alert-danger' });
                  var closeBtn = $('<i></i>', { class: 'close-parent uk-icon-close' });
                  error.append($('<p></p>', { html: 'Internal server error' }));
                  $("#" + tab + "-chart-area-error").append(closeBtn);
                  $("#" + tab + "-chart-area-error").append(error);
                  $("#" + tab + "-chart-area-error").fadeIn(100);
                  return
              }
          }
      })
  }




};
