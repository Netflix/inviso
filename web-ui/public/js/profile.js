/*
 *
 *  Copyright 2014 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

function initProfilePage() {
  $('.navbar-nav .active').removeClass('active');
  $('.nav-profiler').addClass('active');
  processProfilerQueryParams();
}

function processProfilerQueryParams() {
  var uri = URI(window.location.href);

  var query = URI.parseQuery(uri.query());

  var jobs = [].concat(query.job || []);

  _.each(jobs, function(jobId){
    elasticSearchGet(jobId, function(result){
      var data = result._source;
      data.id = result._id;
      var app = apps[data['mapreduce.version'] || 'mr1'];

      incrementLoading();
      app.load(data, function() {decrementLoading();});
    });
  });

  var workflows = [].concat( (query.workflow || []) );

  _.each(workflows, function(){
    searchReferences(workflows, function(jobs){
      _.each(jobs.hits, function(hit){
        var data = hit._source;
        data.id = hit._id;

        var app = apps[data['mapreduce.version'] || 'mr1'];

        incrementLoading();
        app.load(data, function() {decrementLoading();});
      });
    });
  });

}

function incrementLoading() {
  inviso.loading.total(inviso.loading.total() + 1);
  updateProgress();
}

function decrementLoading() {
  var l = inviso.loading;
  l.complete(inviso.loading.complete() + 1);

  updateProgress();

  if (l.complete() == l.total()) {
    setTimeout ( function() {
      l.complete(0);
      l.total(0);
    }, 1000);
  }
}

function updateProgress() {
  var l = inviso.loading;
  var percent = Math.max(l.complete()/l.total() * 100, 5);
  $('.loading-progress').width(percent + '%');
}

function addChart(data) {
  var workflowId = data.workflowId;
  var workflows = inviso.workflows();
  var workflow = null;

  for(var i=0; i<workflows.length; i++) {
    if(workflows[i].id == workflowId) {
      workflow = workflows[i];
      break;
    }
  }

  if(workflow === null) {
    workflow = new function(){
      var self = this;
      this.id = workflowId;
      this.name = data.name;
      this.jobs = ko.observableArray([]);
      this.sortedJobs = ko.computed(function(){
        self.jobs.sort(function(l,r){return l.data.start - r.data.start;});
        return self.jobs;
      });

      return this;
    };

    inviso.workflows.push(workflow);
  }

  workflow.jobs.push(data);

  var $workflowChart = $('#tl-'+btoa(workflow.id).replace(/\=/g, ""));

  if ($workflowChart.data().invisoWorkflow != null)
    $workflowChart.workflow("update");
  else
    $workflowChart.workflow({data:workflow});

  data.app.render(data, $('#'+data.id));
}
