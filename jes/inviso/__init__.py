from snakebite import channel
from snakebite.channel import log_protobuf_message
from snakebite.protobuf.IpcConnectionContext_pb2 import IpcConnectionContextProto

# A little monkey patch to make us run as hadoop
def create_hadoop_connection_context(self):
    '''Creates and seriazlies a IpcConnectionContextProto (not delimited)'''
    context = IpcConnectionContextProto()
    context.userInfo.effectiveUser = 'hadoop'
    context.protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol"

    s_context = context.SerializeToString()
    log_protobuf_message("RequestContext (len: %d)" % len(s_context), context)
    return s_context

channel.SocketRpcChannel.create_connection_context = create_hadoop_connection_context