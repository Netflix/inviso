#!/usr/bin/python

import sys
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

import re
from concurrent.futures import ThreadPoolExecutor
from publish import SQSEventPublisher
from logger import get_logger
import boto.emr


log = get_logger('inviso.jes-mr2-s3')

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
        
        result.append(cluster)
    
    for c in result:
        log.info("Cluster Master : %s" % (c.name))        
            
    return result


def main(argv=None): # IGNORE:C0111
    '''Command line options.'''
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
    
    monitors = [];
    
    for cluster in clusters:
        monitors.append(S3Mr2LogMonitor(jobflow=cluster.flow_id,
                                        cluster_id=cluster.id, 
                                        cluster_name=cluster.name, 
                                        bucket='netflix-dataoven-emr-logs', 
                                        prefix='%s/%s/%s/' % (cluster.id, cluster.flow_id, 'jobs'),
                                        publisher=publisher))
    
    futures = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:    
        for monitor in monitors:
            log.info('Processing ' + monitor.cluster_id)
            futures.append(executor.submit(monitor.run))
            
    log.info('Closing monitors . . .')
    for monitor in monitors:
        monitor.close()
        
    
    return 0


if __name__ == "__main__":
    sys.exit(main())