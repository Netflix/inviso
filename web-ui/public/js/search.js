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

var elasticSearchUrl = null;
var elasticSearchHosts = null;
var elasticSearchClient = null;

var searchFields = [
  { label: 'Timestamp', id: 'timestamp', fields: ['_timestamp'], active: false},
  { label: 'Start', id: 'start', fields: ['mapred.job.start'], active: true},
  { label: 'Stop', id: 'stop', fields: ['mapred.job.stop'], active: true},
  { label: 'Duration', id: 'duration', fields: ['duration'], active: true},
  { label: 'Cluster', id: 'cluster', fields: ['cluster'], active: true},
  { label: 'Job Status', id: 'job_status', fields: ['mapred.job.status'], active: true},
  { label: 'Username', id: 'user', fields: ['user.name','mapreduce.job.user.name'], active: true},
  { label: 'Links', id: 'links', fields: ['_links'], active: true},
  { label: 'Workflow ID', id: 'workflow_id', fields: ['genie.job.id', 'hive.query.id', 'pig.script.id'], active: true},
  { label: 'Genie Name', id: 'genie_name', fields: ['genie.job.name'], active: true},
  { label: 'Genie ID', id: 'genie_id', fields: ['genie.job.id'], active: false},
  { label: 'Genie Version', id: 'genie_version', fields: ['genie.version'], active: false},
  { label: 'MR Job ID', id: 'job_id', fields: ['mapred.job.id','mapreduce.job.id'], active: true},
  { label: 'MR Job Name', id: 'job_name', fields: ['mapred.job.name','mapreduce.job.name'] ,active: true},
  { label: 'Hive Query Id', id: 'query_id', fields: ['hive.query.id'], active: false},
  { label: 'Pig Script Id', id: 'query_id', fields: ['pig.script.id'], active: false},
  { label: 'Document ID', id: 'doc_id', fields: ['_id'], active: false},
  { label: 'Lipstick Server', id: 'lipstick_url', fields: ['lipstick.server.url'], active: false},
  { label: 'Lipstick ID', id: 'lipstick_id', fields: ['lipstick.uuid'], active: false},
  { label: 'Map Tasks', id: 'tasks_map', fields: ['mapred.map.tasks','mapreduce.job.maps'], active: true},
  { label: 'Reduce Tasks', id: 'tasks_reduce', fields: ['mapred.reduce.tasks','mapreduce.job.reduces'], active: true},
  { label: 'Job Tracker', id: 'job_tracker', fields: ['mapred.job.tracker','mapreduce.job.tracker'], active: false},
  { label: 'Job Tracker Address', id: 'job_tracker_addr', fields: ['mapred.job.tracker.http.address','mapreduce.job.tracker.http.address'], active: false},
  { label: 'YARN Proxy', id: 'yarn_proxy', fields: ['yarn.web-proxy.address'], active: false},
  { label: 'Mapreduce Version', id: 'version', fields: ['mapreduce.version'], active: true},
  { label: 'Default FS', id: 'fs', fields: ['fs.defaultFS'], active: false },
  { label: 'History URI', id: 'history_uri', fields: ['history.uri'], active: false },
  { label: 'Incremental', id: 'incremental', fields: ['incremental'], active: false },
  { label: 'Log Agg Dir', id: 'log_dir', fields: ['yarn.nodemanager.remote-app-log-dir'], active: false }
];

var searchFieldsParam = $.map(searchFields, function(entry, i) { return entry.fields.join(','); }).join(',');
var searchFieldMap = {};

$.each(searchFields,function(i, e){
  $.each(e.fields, function(i,p) {
    searchFieldMap[p] = e.id;
  });
});

initElasticSearch();

$(function(){
  //Prevent Page Refresh on form submit
  $('.search-form').submit(false);

  $columnSelector = $('.nav-search-col-button');

  var content = '<div id="column-selector"><ul id="column-select-list">';

  $.each(searchFields, function(i, c) {
    content += '<li class="list-unstyled"><div class="checkbox"><label><input type="checkbox">'+c.label+'</label></div></li>';
  });

  content += '</ul></div>';

  $columnSelector.popover({
    title:'Select Columns',
    html: true,
    content:content,
    placement: 'bottom'
  });

  $columnSelector.on('shown.bs.popover', function() {
    $table = $columnSelector.find('div');
  });
});

function initSearchPage() {
  $('.navbar-nav .active').removeClass('active');
  $('.nav-search').addClass('active');
  initStandardSearch();
  processSearchQueryParams();
}

function focusSearch() {
  $('.search-nav-input').focus();
}

function processSearchQueryParams() {
  var uri = URI(window.location.href);
  var query = URI.parseQuery(uri.query());

  if (query.search != null) {
    $('.search-nav-input').val(query.search);
    executeSearch();
  }
}

function searchReferences(id, callback) {


  var data = {
    q: '\"'+id+'\"',
    fields: ['_timestamp', '_id'],
    _source_include: searchFieldsParam,
    size: 50
  };

  $.ajax({
    url: elasticSearchUrl + "_search",
    dataType: 'json',
    data: data,
    success: function(result) {
      callback(result.hits);
    },
    error: function() {
      $.growl.error({ message: "No data for: " + id });
    }
  });
}

function initStandardSearch() {
  $('.search-form').show();

  $('.search-nav-input').keyup(function(e){
    if(e.keyCode==13) {
      executeSearch();
    }
  });

  $(document).bind('scroll', extendSearch);
}

function disableStandardSearch() {
  $('.search-nav-input').unbind();
  $(document).unbind('scroll', extendSearch);

  $('.search-form').hide();
}

function elasticSearch(query, callback) {
  inviso.search.searching(true);

  var data = {
    q: query,
    fields: '_timestamp',
    _source_include: searchFieldsParam,
    size: 100,
    from: inviso.search.index,
    sort: '_timestamp:desc'
  };

  $.ajax({
    url: elasticSearchUrl + '_search',
    dataType: 'json',
    data: data,
    success: function(result) {
      console.log("Search complete");
      inviso.search.searching(false);

      inviso.search.index += result.hits.hits.length;
      inviso.search.total = result.hits.total;

      callback(result.hits.hits);
    },
    error: function() {
      inviso.search.searching(false);

      $.growl.error({ message: "No data for: " + query });
    }
  });
}

function elasticSearchGet(docId, callback) {
  var data = {
    fields: '_id,_timestamp',
    _source_include: searchFieldsParam,
    _source: true
  };

  $.ajax({
    url: elasticSearchUrl + docId,
    dataType: 'json',
    data: data,
    success: function(result) {
      callback(result);
    },
    error: function() {
      $.growl.error({ message: "No document for: " + docId });
    }
  });
}

function executeSearch(extend) {
  if(!extend) {
    inviso.search.reset();
  }

  var query = $.trim($('.search-nav-input').val());

  if(query === '') {
    return;
  }

  elasticSearch(query, function(data){
    $.each(data, function(i,hit) {
      hit._source._timestamp = moment(hit.fields._timestamp).format('YYYY-MM-DD HH:mm:ss (UTC ZZ)');
      hit._source['mapred.job.id'] = hit._id;

      var searchResult = {};

      $.each(hit._source, function(field, value){
        searchResult[searchFieldMap[field]] = value;
      });

      appendSearchResults(searchResult);
      inviso.search.results().push(searchResult);
    });

    inviso.search.results.valueHasMutated();
  });
}

function extendSearch() {
  if(inviso.search.results().length === 0) {
    return;
  }

  if(inviso.search.canSearch() && $(document).scrollTop() + $(window).height() >= $(document).height()) {
    console.log("Expanding search");
    executeSearch(true);
  }
}

//Have to do this manually because KO is too slow.
function appendSearchResults(r) {
  var app = inviso.apps[r.version!=null?r.version:'mr1'];

  var $tr = $('<tr class="search-result"></tr>');

  $.each(inviso.search.columns, function(i, col){
    if(!col.active) {
        return;
    }

    var $td = app.renderSearchResult(col, r);
    $tr.append($td);
  });

  $('.search-results').append($tr);
}
