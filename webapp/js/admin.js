$(function() {
  // Humanize size value
  var humanvalue = ['B','KB','MB','GB']
  function humanize(bytes) {
    curbytes = bytes
    iterations = 0
    while (curbytes>1024) {
      iterations++
      curbytes=curbytes/1024
      }
    return curbytes.toFixed(1) + ' ' + humanvalue[iterations]
  };

  $.parseJSON = _.wrap($.parseJSON, function(func, text){
    text = text.replace(/([^\\])":\s*([^,}{"]+ [^,}{"]+)/g, '$1":"$2"')
    return func(text);
  });
  
  function strToJmx(type, value) {
    switch(type) {
      case "int":
      case "long":
        var num = Number(value);
        if (isNaN(num))
          throw 'Cannot convert "' + value + '" to a number.';
        return num;
      case "boolean":
        if (/true/i.test(value))
          return true;
        else if (/false/i.test(value))
          return false;
        else
          throw 'Cannot convert "' + value + '" to a boolean.';
      case "java.lang.String":
        return value; 
      case "java.util.Date":
        return new Date(value);
      default:
        return $.parseJSON(value);
    }
  };

  function generateJmxUrl(jmxBase, jmxUrl) {
    var url = null;
    if(jmxBase)
      url = jmxBase;
    if (jmxUrl) {
      if (url != null)
        url += encodeURIComponent(jmxUrl);
      else
        url = jmxUrl;
    }

    return url;
  }

  window.JMXModel = Backbone.Model.extend({
    defaults: {
      req: {type: 'list'},
      lazy: false,
      jmxBase: 'sensei/admin/jmx/'
    },

    url: function() {
      return generateJmxUrl(this.get('jmxBase'), this.get('jmxUrl'));
    },

    initialize: function() {
      _.bindAll(this, 'read', 'create', 'update');
      JMXModel.__super__.initialize.call(this);
      if (!this.get('lazy'))
        this.fetch();
    },

    read: function() {
      //console.log('>>> jmx read called.');
      //console.log('>>> jmx url: ' + this.url());
      var jmx = new Jolokia({url: this.url()});
      var req = _.clone(this.get('req'));
      if (req.type != 'list' && req.type != 'exec')
        req.type = 'read';
      res = jmx.request(req, {method: "post"});
      if (_.isUndefined(res.id)) {
        if (_.isUndefined(this.id))
          res.id = this.cid;
      }
      return res;
    },

    create: function() {
      return this.update();
    },

    update: function() {
      var jmx = new Jolokia({url: this.url()});
      var req = _.clone(this.get('req'));
      if (req.type != 'list' && req.type != 'exec') {
        req.type = 'write';
        req.value = this.get('value');
      }
      res = jmx.request(req, {method: "post"});
      if (_.isUndefined(res.id)) {
        if (_.isUndefined(this.id))
          res.id = this.cid;
      }

      if (req.type != 'exec') {
        // Jmx may return old value.
        res.value = req.value;
      }
      return res;
    }
  });

  window.JMXAttrModel = JMXModel.extend({
  });

  window.JMXAttrCollection = Backbone.Collection.extend({
    model: JMXAttrModel
  });

  window.JMXAttrView = Backbone.View.extend({
    template: $('#jmx-attr-tmpl').html(),

    className: 'jmx-item',

    initialize: function() {
      _.bindAll(this, 'render', 'save', 'syncFromEditor');
      this.model.bind('change', this.render);
      this.model.view = this;
    },

    save: function() {
      humanMsg.displayMsg("Saving...");
      try {
        this.syncFromEditor();
      }
      catch(e) {
        humanMsg.displayMsg(e);
        return;
      }
      this.model.save(this.model, {
        success: function(m, res) {
          humanMsg.displayMsg("Saved");
        },
        error: function(m, res) {
          humanMsg.displayMsg("Error: " + res);
        }
      });
    },

    syncFromEditor: function() {
      this.model.set({value: strToJmx(this.model.get('attr').type, this.$('.attr-editor').val())});
    },

    render: function() {
      //console.log(this.model.get('attr'));
      //console.log(this.model.toJSON());
      $(this.el).html($.mustache(this.template, this.model.toJSON()));
      this.$('.save').bind('click', this.save);
      return this;
    }
  });

  window.JMXOpModel = JMXModel.extend({
  });

  window.JMXOpCollection = Backbone.Collection.extend({
    model: JMXOpModel
  });

  window.JMXOpView = Backbone.View.extend({
    template: $('#jmx-op-tmpl').html(),

    initialize: function() {
      _.bindAll(this, 'render', 'save', 'syncFromEditor');
      this.model.bind('change', this.render);
      this.model.view = this;
    },

    save: function () {
      humanMsg.displayMsg("Calling...");
      try {
        this.syncFromEditor();
      }
      catch(e) {
        humanMsg.displayMsg(e);
        return;
      }
      this.model.save(this.model, {
        success: function(m, res) {
          if (m.get('op').ret != 'void')
            humanMsg.displayMsg(m.get('value'));
          else
            humanMsg.displayMsg("Success");
        },
        error: function(m, res) {
          humanMsg.displayMsg("Error: " + res);
        }
      });
    },

    syncFromEditor: function() {
      var me = this;
      var args = [];
      me.$('.arg').each(function(index, arg){
        args.push(strToJmx(me.model.get('op').args[index].type, $(arg).val()));
      });

      me.model.get('req').arguments = args;
    },

    render: function() {
      //console.log(this.model.toJSON());
      if (_.isUndefined(this.rendered)) {
        $(this.el).html($.mustache(this.template, this.model.toJSON()));
        this.$('.call').bind('click', this.save);
        this.rendered = true;
      }
      return this;
    }
  });

  window.MBeanModel = Backbone.Model.extend({
  });

  window.MBeanView = Backbone.View.extend({
    template: $('#jmx-mbean-tmpl').html(),

    initialize: function () {
      _.bindAll(this, 'render', 'refresh');
      this.model.view = this;
    },

    refresh: function() {
      //console.log('>>> refresh called.');
      this.model.get('attrs').forEach(function(attr){
        attr.fetch();
      });
    },

    render: function() {
      //console.log(this.model);
      this.options.parentView.$('.jmx-main-area').children().hide();
      if (_.isUndefined(this.rendered)) {
        $(this.el).html($.mustache(this.template, this.model.toJSON()));
        var me = this;
        this.model.get('attrs').forEach(function(attr) {
          me.$('.attrs').append(attr.fetch().view.el);
        });
        this.model.get('ops').forEach(function(op) {
          me.$('.ops').append(op.view.render().el);
        });
        this.refreshInput = this.$('.refresh');
        this.refreshInput.bind('click', this.refresh);
        this.options.parentView.$('.jmx-main-area').append(this.el);
        this.rendered = true;
      }
      $(this.el).show();
    }
  });

  window.JMXTreeViewModel = JMXModel.extend({
    defaults: _.defaults({
      req: {type: 'list'}
    }, JMXModel.prototype.defaults),

    initialize: function() {
      _.bindAll(this, 'toTree');
      JMXTreeViewModel.__super__.initialize.call(this);
    },

    toTree: function() {
      var data = {
        "json_data" : {data: []},
        "plugins" : [ "themes", "json_data", "ui" ]
      };
      var jmx = this.get('value');
      for (var folderName in jmx) {
        if(_.isString(folderName)) {
          var folder = jmx[folderName];
          var folderNode = {data: folderName, metadata: folder};
          folderNode.children = [];
          for (var mbeanName in folder) {
            if (_.isString(mbeanName)) {
              var mbean = folder[mbeanName];
              mbean.objectName = folderName+':'+mbeanName;

              var attrs = new JMXAttrCollection();
              if (!_.isUndefined(mbean.attr)) {
                for (var name in mbean.attr) {
                  if (_.isString(name)) {
                    var attr = new JMXAttrModel({
                      lazy: true,
                      name: name,
                      attr: mbean.attr[name],
                      req: {mbean: mbean.objectName, attribute: name},
                      jmxUrl: this.get('jmxUrl')
                    });
                    var view = new JMXAttrView({model: attr});
                    attrs.add(attr);
                  }
                }
              }

              var ops = new JMXOpCollection();
              if (!_.isUndefined(mbean.op)) {
                for (var name in mbean.op) {
                  if (_.isString(name)) {
                    var op = new JMXOpModel({
                      lazy: true,
                      name: name,
                      op: mbean.op[name],
                      req: {type: 'exec', mbean: mbean.objectName, operation: name},
                      jmxUrl: this.get('jmxUrl')
                    });
                    var view = new JMXOpView({model: op});
                    ops.add(op);
                  }
                }
              }

              mbean.model = new MBeanModel({attrs: attrs, ops: ops});
              var view = new MBeanView({model: mbean.model, parentView: this.view});

              var mbeanNode = {data: mbeanName.replace(/,/ig, ', '), metadata: mbean};
              folderNode.children.push(mbeanNode);
              /*
              mbeanNode.children = [];
              if (!_.isUndefined(mbean.attr)) {
                var attrNode = {data: 'attr', metadata: mbean.attr};
                mbeanNode.children.push(attrNode);
                attrNode.children = [];
                for (var name in mbean.attr) {
                  if (_.isString(name)) {
                    var val = mbean.attr[name];
                    var node = {data: name, metadata: val};
                    attrNode.children.push(node);
                  }
                }
              }
              if (!_.isUndefined(mbean.op)) {
                var opNode = {data: 'op', metadata: mbean.op};
                mbeanNode.children.push(opNode);
                opNode.children = [];
                for (var name in mbean.op) {
                  if (_.isString(name)) {
                    var val = mbean.op[name];
                    var node = {data: name, metadata: val};
                    opNode.children.push(node);
                  }
                }
              }
              */
            }
          }
          data.json_data.data.push(folderNode);
        }
      }
      return data;
    }
  });

  window.JMXTimedDataModel = JMXModel.extend({
    defaults: _.defaults({
      sampleSize: 100,
      viewOptions: {},
      interval: 5000
    }, JMXModel.prototype.defaults),
    initialize: function() {
      _.bindAll(this, 'refresh', 'newData', 'format');
      this.bind('change', this.newData);
      //console.log('>>> JMXTimedDataModel.initialize');
      this.dataTable = new google.visualization.DataTable();
      this.dataTable.addColumn('datetime', 'Time');
      this.dataTable.addColumn(this.get('dataType'), this.get('dataLabel'));

      JMXTimedDataModel.__super__.initialize.call(this);

      _.delay(this.refresh, 1000);

      window.setInterval(this.refresh, this.get('interval'));
    },

    refresh: function() {
      //console.log('>>> timed data refresh called.');
      this.fetch();
    },

    newData: function() {
      if (!_.isUndefined(this.get('data')))
        this.dataTable.addRow([new Date(), this.get('data')]);
        if (this.dataTable.getNumberOfRows() > this.get('sampleSize')) {
          this.dataTable.removeRow(0);
        }
    },

    parse: function(res) {
      res.data = res.value;
      return res;
    },

    format: function() {
      var formatter = new google.visualization.DateFormat({pattern: "HH:mm:ss"});
      formatter.format(this.dataTable, 0);
    }
  });

  window.RTModel = JMXTimedDataModel.extend({
    defaults: _.defaults({
      serviceName: 'sensei',
      dataType: 'number',
      dataLabel: 'Response Time (ms)'
    }, JMXTimedDataModel.prototype.defaults),

    initialize: function() {
      RTModel.__super__.initialize.call(this);
      //console.log('>>> this.get("serviceName")', this.get('serviceName'));
      this.set({
        req: {
          mbean: "com.linkedin.norbert:type=NetworkServerStatistics,service="+this.get('serviceName'),
          attribute: "AverageRequestProcessingTime"
        }
      });
    }
  });

  window.RPSModel = JMXTimedDataModel.extend({
    defaults: _.defaults({
      serviceName: 'sensei',
      dataType: 'number',
      dataLabel: 'Requests Per Second'
    }, JMXTimedDataModel.prototype.defaults),

    initialize: function() {
      RPSModel.__super__.initialize.call(this);
      this.set({
        req: {
          mbean: "com.linkedin.norbert:type=NetworkServerStatistics,service="+this.get('serviceName'),
          attribute: "RequestsPerSecond"
        }
      });
    }
  });

  window.EPMModel = JMXTimedDataModel.extend({
    defaults: _.defaults({
      req: {mbean: "com.senseidb:indexing-manager=stream-data-provider", attribute: "EventsPerMinute"},
      dataType: 'number',
      dataLabel: 'Events Per Minute',
      interval: 60000
    }, JMXTimedDataModel.prototype.defaults),

    initialize: function() {
      EPMModel.__super__.initialize.call(this);
    }
  });

  window.CPUModel = JMXTimedDataModel.extend({
    defaults: _.defaults({
      req: {mbean: "java.lang:type=OperatingSystem", attribute: "ProcessCpuTime"},
      dataType: 'number',
      dataLabel: 'CPU Usage (%)'
    }, JMXTimedDataModel.prototype.defaults),

    initialize: function() {
      _.bindAll(this, 'format');
      CPUModel.__super__.initialize.call(this);
    },

    parse: function(res) {
      var now = new Date().getTime();
      if (!_.isUndefined(this.preCPUTime)) {
        res.data = (res.value - this.preCPUTime)/10000.0/(now - this.preTime)
      }
      this.preTime = now;
      this.preCPUTime = res.value;

      return res;
    },

    format: function() {
      CPUModel.__super__.format.call(this);
      var formatter = new google.visualization.NumberFormat({suffix: "%"});
      formatter.format(this.dataTable, 1);
      //console.log(this.dataTable.toJSON());
    }
  });

  window.MemModel = JMXTimedDataModel.extend({
    defaults: _.defaults({
      req: {mbean: "java.lang:type=Memory", attribute: "HeapMemoryUsage"},
      dataType: 'number',
      dataLabel: 'Heap Mem Usage'
    }, JMXTimedDataModel.prototype.defaults),

    initialize: function() {
      MemModel.__super__.initialize.call(this);
    },

    parse: function(res) {
      res.data = res.value.used;
      return res;
    },

    format: function() {
      MemModel.__super__.format.call(this);
      for (var i=0; i<this.dataTable.getNumberOfRows(); ++i) {
        this.dataTable.setCell(i, 1, this.dataTable.getValue(i, 1), humanize(this.dataTable.getValue(i, 1)));
      }
      //console.log(this.dataTable.toJSON());
    }
  });

  window.TimedDataView = Backbone.View.extend({
    initialize: function() {
      _.bindAll(this, 'render');
      this.model.bind('change', this.render);
      this.model.view = this;
    },

    render: function() {
      if (_.isUndefined(this.chart))
        this.chart = new google.visualization.LineChart(this.el);

      if (_.isFunction(this.model.format))
        this.model.format();

      if ($(this.el).is(":visible"))
        this.chart.draw(this.model.dataTable, _.defaults({width: '100%', height: 200, title: this.model.get('dataLabel')}, this.model.get('viewOptions')));
    }
  });

  window.SenseiNode = Backbone.Model.extend({
    initialize: function() {
      //console.log('>>> SenseiNode.initialize called');
      this.jmxModel = new JMXTreeViewModel({jmxUrl: this.get('adminlink')});
      new JMXView({model: this.jmxModel});
    }
  });

  window.SenseiNodes = Backbone.Collection.extend({
    model: SenseiNode,

    initialize: function() {
      _.bindAll(this, 'added', 'removed');
      this.bind('add', this.added);
      this.bind('remove', this.removed);
    },

    comparator: function(node) {
      return node.id;
    },

    added: function(node) {
      if (!node.get('added')) {
        node.set({added: true});
        $(node.overview.parentView.el).append(node.overview.render().el);
      }
    },

    removed: function(node) {
      if (node.get('added')) {
        node.set({added: false});
        $(node.overview.render().el).detach();
      }
    }
  });

  window.SenseiNodeOverview = Backbone.View.extend({
    className: 'node-overview',

    template: $('#node-overview-tmpl').html(),

    initialize: function() {
      _.bindAll(this, 'render', 'initializeSubViews');
      this.model.overview = this;
    },

    initializeSubViews: function() {
      if (_.isUndefined(this.model.rt)) {
        var jmx = this.model.jmxModel.get('value');
        var serviceName = 'sensei';
        try {
          for(key in jmx['com.linkedin.norbert']) {
            if (_.isString(key)) {
              var m = /serviceName=([^,]+)/.exec(key);
              if (m)
                serviceName = m[1];
            }
          }
        }
        catch(e) {
          console.log(e);
        }
        //console.log('>>> serviceName: ' + serviceName);

        this.model.rt = new RTModel({serviceName: serviceName, jmxUrl: this.model.get('adminlink')});
        new TimedDataView({model: this.model.rt, el: this.$('.rt').get(0)});

        this.model.rps = new RPSModel({serviceName: serviceName, jmxUrl: this.model.get('adminlink')});
        new TimedDataView({model: this.model.rps, el: this.$('.rps').get(0)});

        this.model.epm = new EPMModel({serviceName: serviceName, jmxUrl: this.model.get('adminlink')});
        new TimedDataView({model: this.model.epm, el: this.$('.epm').get(0)});

        this.model.cpu = new CPUModel({serviceName: serviceName, jmxUrl: this.model.get('adminlink')});
        new TimedDataView({model: this.model.cpu, el: this.$('.cpu').get(0)});

        this.model.mem = new MemModel({serviceName: serviceName, jmxUrl: this.model.get('adminlink')});
        new TimedDataView({model: this.model.mem, el: this.$('.mem').get(0)});
      }
    },

    render: function() {
      //console.log('>>> SenseiNodeOverview render');
      $(this.el).html($.mustache(this.template, this.model.toJSON()));

      this.initializeSubViews();

      return this;
    }
  });

  window.JMXView = Backbone.View.extend({
    template: $('#jmx-tmpl').html(),

    initialize: function() {
      _.bindAll(this, 'render');
      this.model.view = this;
    },

    render: function() {
      var me = this;
      $(this.el).html($.mustache(this.template, this.model.toJSON()));
      $('#main-container').append(this.el);
      this.$('.tree-nav').jstree(this.model.toTree()).bind('select_node.jstree', function(e, data) {
        me.$('.jmx-main-area').children().hide();
        try {
          if (!_.isUndefined(data.inst.get_json()[0].metadata.model))
            data.inst.get_json()[0].metadata.model.view.render();
        }
        catch(e) {
          console.log(e);
        }
      });
    }
  });

  window.SenseiNodesView = Backbone.View.extend({
    initialize: function() {
    },

    render: function() {
      $('#main-container').append(this.el);
    }
  });

  window.SenseiSystemInfo = Backbone.Model.extend({
    defaults: {
      dateToLocaleString: function(text) {
        return function(text, render) {
          return new Date(parseInt(render(text))).toLocaleString();
        }
      }
    },

    url: function() {
      return 'sensei/sysinfo';
    }
  });

  window.SenseiSystemInfoView = Backbone.View.extend({
    el: $("#sysinfo"),

    template: $('#sysinfo-tmpl').html(),

    initialize: function() {
      _.bindAll(this, 'infoChanged', 'render');
      this.model.bind('change', this.infoChanged);
      this.model.view = this;

      this.model.nodes = new SenseiNodes();
      this.model.nodesView = new SenseiNodesView({collection: this.model.nodes});
    },

    updateSenseiNodes: function(clusterinfo) {
      //console.log('>>> updateSenseiNodes');
      var me = this;
      clusterinfo.forEach(function(node) {
        var nodeView = new SenseiNodeOverview({model: node});
        nodeView.parentView = me.model.nodesView;
      });

      me.model.nodes.remove(me.model.nodes.reject(function(node) {
        return clusterinfo.get(node.id);
      }));
      me.model.nodes.add(clusterinfo.reject(function(node){
        return me.model.nodes.get(node.id);
      }));
    },

    infoChanged: function() {
      //console.log('>>> infoChanged');
      this.updateSenseiNodes(new SenseiNodes(this.model.get('clusterinfo'), {silent: true}));
      this.render();
    },

    render: function() {
      var jsonModel = this.model.toJSON();
      // Localize lastmodified:
      $(this.el).html($.mustache(this.template, jsonModel));
    }
  });

  var baseSync = Backbone.sync;

  Backbone.sync = function(method, model, success, error) {
    _.bind(baseSync, this);

    var resp;

    switch (method) {
      case "read":
        if (model.read)
          resp = model.read();
        else
          return baseSync(method, model, success, error);
        break;
      case "create":
        if (model.create)
          resp = model.create();
        else
          return baseSync(method, model, success, error);
        break;
      case "update":
        if (model.update)
          resp = model.update();
        else
          return baseSync(method, model, success, error);
        break;
      case "delete":
        if (model.delete)
          resp = model.delete();
        else
          return baseSync(method, model, success, error);
        break;
    }

    if (resp) {
      success(resp);
    } else {
      error("Record not found");
    }
  };

  var AdminSpace = Backbone.Controller.extend({
    routes: {
      'overview':         'overview', //#overview
      'node/:id/jmx':     'jmx', //#node/1/jmx
      '.*':               'index', //#node/1/jmx
    },

    initialize: function() {
      _.bindAll(this, 'overview', 'jmx');
      var info = new SenseiSystemInfo();
      window.senseiSysInfo = new SenseiSystemInfoView({model: info});
      info.fetch();
    },

    index: function() {
      //console.log('>>> index called');
      this.overview();
      this.saveLocation('overview');
    },

    overview: function() {
      //console.log('>>> overview called');
      $('#nav-node').text("");
      $('#main-container').children().hide();

      if (_.isUndefined(this.overviewRendered)) {
        senseiSysInfo.model.nodesView.render();
        this.overviewRendered = true;
      }

      $(senseiSysInfo.model.nodesView.el).show();
    },

    jmx: function(id) {
      $('#nav-node').text(" > Node " + id);
      //console.log('>>> node '+id+' jmx called');
      $('#main-container').children().hide();
      if (_.isUndefined(senseiSysInfo.model.nodes.get(id))) {
        _.delay(this.jmx, 1000, id);
        return;
      }
      var jmxView = senseiSysInfo.model.nodes.get(id).jmxModel.view;

      var ukey = 'jmx' + id;
      if (_.isUndefined(this[ukey])) {
        jmxView.render();
        this[ukey] = true;
      }

      $(jmxView.el).show();
    }
  });

  window.adminSpace = new AdminSpace();
  Backbone.history.start();
});
