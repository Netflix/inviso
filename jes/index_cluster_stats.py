import sys

from elasticsearch.helpers import bulk

import re
import requests
import arrow
from inviso.util import get_logger
import settings
import boto.emr


log = get_logger('inviso.cluster')
es_index = 'inviso-cluster'

def index_apps(es, cluster, info):
    apps = requests.get('http://%s:%s/ws/v1/cluster/apps?state=RUNNING' % (cluster.host, cluster.port), headers = {'ACCEPT':'application/json'}).json().get('apps')
    
    if not apps:
        log.info(cluster.name + ': no applications running.')
        return
    
    apps = apps['app']
    
    documents = [
        {
            '_op_type': 'index',
            '_index': es_index,
            '_type': 'applications',
            '_id':  '_heartbeat_%s' % (info['timestamp'],),
            '_ttl': '30d',
            '_timestamp': info['timestamp'],
            '_source': { 'heartbeat': True }
        }
    ]
    
    for app in apps:
        app.update(info)
        
        documents.append({
            '_op_type': 'index',
            '_index': es_index,
            '_type': 'applications',
            '_id':  '%s_%s' % (app['id'],  info['timestamp']),
            '_ttl': '30d',
            '_timestamp': info['timestamp'],
            '_source': app
        })
      
    log.info('%s: Indexing %s documents' % (cluster.name, len(documents)))            
    log.debug(bulk(es, documents, stats_only=True));

def index_metrics(es, cluster, info):
    metrics = requests.get('http://%s:%s/ws/v1/cluster/metrics' % (cluster.host, cluster.port), headers = {'ACCEPT':'application/json'}).json()['clusterMetrics']
    metrics.update(info)
    
    r = es.index(index=es_index, 
                    doc_type='metrics', 
                    id= '%s_%s' % (cluster.id, info['timestamp']),
                    ttl='30d',
                    timestamp=info['timestamp'],
                    body=metrics)
    log.debug(r)

def index_scheduler(es, cluster, info):
    scheduler = requests.get('http://%s:%s/ws/v1/cluster/scheduler' % (cluster.host, cluster.port), headers = {'ACCEPT':'application/json'}).json()['scheduler']['schedulerInfo']['rootQueue']
    scheduler.update(info)

    r = es.index(index=es_index, 
                    doc_type='scheduler', 
                    id= '%s_%s' % (cluster.id, info['timestamp']),
                    ttl='30d',
                    timestamp=info['timestamp'],
                    body=scheduler)
    log.debug(r)

def index_stats(clusters):
    es = settings.elasticsearch
    timestamp = arrow.utcnow().floor('minute').timestamp * 1000
    
    for cluster in clusters:
        try:
            info = {
                'timestamp': timestamp,
                'cluster': cluster.name,
                'cluster.id': cluster.id,
                'host': cluster.host,
		'port': cluster.port
            }
            
            index_apps(es, cluster, info)
            index_metrics(es, cluster, info)

            #FairScheduler
            #index_scheduler(es, cluster, info)
        except Exception as e:
            log.error('Error processing: ' + cluster.name)
            log.exception(e)
            
    es.indices.refresh(index=es_index)

def main():
    index_stats(settings.clusters)
    
if __name__ == '__main__':
    sys.exit(main())
