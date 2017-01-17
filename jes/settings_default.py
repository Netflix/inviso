import socket
import requests
from urllib import urlencode
from elasticsearch import Elasticsearch

from inviso import util
from inviso.monitor import Cluster
from inviso.handler import IndexHandler
from inviso.publish import DirectPublisher

from snakebite import channel
from snakebite.channel import log_protobuf_message
from snakebite.protobuf.IpcConnectionContext_pb2 import IpcConnectionContextProto
from inviso.conf_reader import getConfElement

log = util.get_logger("inviso-settings")

# A monkey patch to make us run as hadoop
def create_hadoop_connection_context(self):
    '''Creates and seriazlies a IpcConnectionContextProto (not delimited)'''
    context = IpcConnectionContextProto()
    context.userInfo.effectiveUser = getConfElement('cluster','effective_user')
    context.protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol"

    s_context = context.SerializeToString()
    log_protobuf_message("RequestContext (len: %d)" % len(s_context), context)
    return s_context
channel.SocketRpcChannel.create_connection_context = create_hadoop_connection_context


def genie_lookup(job_id):
    path = '/genie/v0/jobs/' + job_id
    headers = {'content-type': 'application/json', 'Accept': 'application/json'}
    response = requests.request('GET', genie_host + path, headers=headers).json()

    if not response:
        return None

    return response.json()


def genie_clusters():
    path = 'genie/v0/config/cluster/'

    headers = {'content-type': 'application/json', 'Accept': 'application/json'}
    response = requests.request('GET', genie_host + path, headers=headers).json()

    if not response:
        return None

    return response.json()


def inviso_trace(job_id, uri, version='mr1', summary=True):
    path = '/inviso/%s/v0/trace/load/%s?%s&summary=%s' % (version, job_id, urlencode({'path': uri}), summary)
    headers = {'content-type': 'application/json', 'Accept': 'application/json'}
    response = requests.request('GET', 'http://' +inviso_host + path, headers=headers)
    
    if not response:
        return None
    
    return response.json()

inviso_host = getConfElement('inviso','inviso_host')
genie_host = getConfElement('inviso','genie_host')
elasticsearch_hosts = [{'host': getConfElement('elastic_search','host'), 'port': int(getConfElement('elastic_search','port'))}]
elasticsearch = Elasticsearch(elasticsearch_hosts)
clusters = [
    Cluster(id=getConfElement('cluster','cluster_id'), name=getConfElement('cluster','cluster_name'), host=socket.getfqdn(getConfElement('cluster','resource_manager')), port=getConfElement('cluster','resource_manager_port'),namenode=getConfElement('cluster','namenode'),namenode_port=int(getConfElement('cluster','namenode_port')),history_server=getConfElement('cluster','history_server_dir'))
]

handler = IndexHandler(trace=inviso_trace, elasticsearch=elasticsearch)
publisher = DirectPublisher(handler=handler)
