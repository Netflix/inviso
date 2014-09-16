#!/usr/bin/python

import sys
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

import re
from publish import SQSEventPublisher
from inviso import util
import boto.emr
from boto.emr.connection import EmrConnection
from inviso.monitor import HdfsMr2LogMonitor, Cluster

log = util.get_logger('inviso.jes-hdfs')


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
        cluster.host = flow.masterpublicdnsname
        
        result.append(cluster)
    
    for c in result:
        log.info("Cluster Master : %s %s" % (c.name,c.master_host))        
            
    return result


def main(argv=None):
    """Command line options."""
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    # Setup argument parser
    parser = ArgumentParser(description='Job Event Service', formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-q", "--queue", dest="queues", action="append", help="queue names to publish events")
    parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
    
    # Process arguments
    args = parser.parse_args()
    
    verbose = args.verbose
    queues = args.queues
    
    if verbose > 0:
        print("Verbose mode on")
        print("Queues: " + str(queues))

    if len(queues) == 0:
        print("Please define queues to publish to with the '-q' option.")
        return 1
    
    publisher = SQSEventPublisher(queues=queues)
    
    clusters = locate_mr2_clusters()
    
    monitors = []
    
    for cluster in clusters:
        monitors.append(HdfsMr2LogMonitor(jobflow=cluster.flow_id,
                                          cluster_id=cluster.id, 
                                          cluster_name=cluster.name, 
                                          host=cluster.host,
                                          auto_create=True, 
                                          publisher=publisher))
    
    for monitor in monitors:
        try:
            monitor.run()
            monitor.close()
        except Exception as e:
            log.exception(e)
        
    
    return 0


if __name__ == "__main__":
    sys.exit(main())