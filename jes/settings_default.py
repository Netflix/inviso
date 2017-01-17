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

log = util.get_logger("inviso-settings")

# A monkey patch to make us run as hadoop
def create_hadoop_connection_context(self):
    '''Creates and seriazlies a IpcConnectionContextProto (not delimited)'''
    context = IpcConnectionContextProto()
    context.userInfo.effectiveUser = 'hadoop'
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

inviso_host = 'localhost:8080'
genie_host = 'localhost:8080'
elasticsearch_hosts = [{'host': 'localhost', 'port': 9200}]
elasticsearch = Elasticsearch(elasticsearch_hosts)
clusters = [
    Cluster(
      id='cluster_1',
      name='cluster_1',
      host=socket.getfqdn(),
      namenode_rpc_port = 9000,
      resourcemanager_webapp_port = 9026
    )
]

handler = IndexHandler(trace=inviso_trace, elasticsearch=elasticsearch)
publisher = DirectPublisher(handler=handler)
