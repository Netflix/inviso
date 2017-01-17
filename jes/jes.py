#!/usr/bin/python

import sys
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from inviso.publish import DirectPublisher
from inviso import util
from inviso.monitor import HdfsMr2LogMonitor

log = util.get_logger('inviso.jes')

try:
    import settings
except Exception as e:
    log.error('settings.py not found')
    log.exception(e)
    sys.exit(1)


def main():
    publisher = settings.publisher

    monitors = []
    
    for cluster in settings.clusters:
        monitors.append(HdfsMr2LogMonitor(jobflow=cluster.id,
                                          cluster_id=cluster.id, 
                                          cluster_name=cluster.name, 
                                          host=cluster.namenode,
					  port=cluster.namenode_port,
					  log_path=cluster.history_server,
                                          publisher=publisher,
                                          elasticsearch=settings.elasticsearch))
    
    for monitor in monitors:
        try:
            monitor.run()
            monitor.close()
        except Exception as e:
            log.exception(e)

    return 0


if __name__ == "__main__":
    sys.exit(main())
