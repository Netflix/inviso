import sys

from elasticsearch.helpers import bulk

import re
import requests
import arrow
from logger import get_logger
from settings_default import elasticsearch
import boto.emr


log = get_logger('inviso.jes-mr2-s3')
es_index = 'inviso-cluster'

class Cluster:
    def __init__(self):
        pass

def locate_mr2_clusters():
    result = []
    
    emr = boto.emr.connect_to_region('us-east-1')

    for flow in emr.describe_jobflows(states=['WAITING']):
        if not flow.hadoopversion.startswith('2'):
            continue
        
        cluster = Cluster()
        cluster.flow_id = flow.jobflowid
        cluster.name = re.sub('_([0-9]+)/?', '', flow.name)
        cluster.id = flow.name
        cluster.master = flow.masterpublicdnsname
        
        result.append(cluster)
    
    for c in result:
        log.info("Cluster Master : %s" % (c.name))        
            
    return result

def index_apps(es, cluster, info):
    apps = requests.get('http://%s:%s/ws/v1/cluster/apps?state=RUNNING' % (cluster.master, '9026'), headers = {'ACCEPT':'application/json'}).json().get('apps')
    
    if not apps:
        log.info(cluster.name + ': no applications running.')
        return
    
    apps = apps['app']
    
    documents = []
    
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
    metrics = requests.get('http://%s:%s/ws/v1/cluster/metrics' % (cluster.master, '9026'), headers = {'ACCEPT':'application/json'}).json()['clusterMetrics']
    metrics.update(info)
    
    r = es.index(index=es_index, 
                    doc_type='metrics', 
                    id= '%s_%s' % (cluster.id, info['timestamp']),
                    ttl='30d',
                    timestamp=info['timestamp'],
                    body=metrics)
    log.debug(r)

def index_scheduler(es, cluster, info):
    scheduler = requests.get('http://%s:%s/ws/v1/cluster/scheduler' % (cluster.master, '9026'), headers = {'ACCEPT':'application/json'}).json()['scheduler']['schedulerInfo']['rootQueue']
    scheduler.update(info)
    
    r = es.index(index=es_index, 
                    doc_type='scheduler', 
                    id= '%s_%s' % (cluster.id, info['timestamp']),
                    ttl='30d',
                    timestamp=info['timestamp'],
                    body=scheduler)
    log.debug(r)

def index_stats(clusters):
    es = elasticsearch()
    timestamp = arrow.utcnow().floor('minute').timestamp * 1000
    
    for cluster in clusters:
        try:
            info = {
                'timestamp': timestamp,
                'cluster': cluster.name,
                'cluster.id': cluster.id,
                'master': cluster.master             
            }
            
            index_apps(es, cluster, info)
            index_metrics(es, cluster, info)
            index_scheduler(es, cluster, info)
        except Exception as e:
            log.error('Error processing: ' + cluster.name)
            log.exception(e)
            
    es.indices.refresh(index=es_index)
            
def main():
    index_stats(locate_mr2_clusters())
    
if __name__ == '__main__':
    sys.exit(main())
