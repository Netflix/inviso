from datetime import datetime

import pytz
import boto
from boto.s3.bucket import Bucket
import re
import os
import arrow
from util import get_logger

from snakebite.client import Client

log = get_logger('inviso-monitor')

job_pattern = re.compile('job_[0-9]+_[0-9]+', re.IGNORECASE)
EPOCH = datetime(1970, 1, 1, tzinfo=pytz.UTC)


class Cluster:
    def __init__(self, id, name, host):
        self.id = id
        self.name = name
        self.host = host

class Monitor(object):
    def __init__(self, publisher=None, **kwargs):
        self.publisher = publisher
    
    def run(self):
        raise NotImplementedError("Subclass to Implement")
    
    def close(self):
        log.warn('destroy() not implemented.')


class TimestampMonitor(Monitor):
    def __init__(self, timestamp_file, auto_create=False, **kwargs):
        super(TimestampMonitor, self).__init__(**kwargs)
        
        self.timestamp_file = timestamp_file
        
        if auto_create and not os.path.isfile(timestamp_file):
            log.warn("Creating missing timestamp file: " + timestamp_file)
            self.touch(timestamp_file, EPOCH)
        
        self.current_run = pytz.UTC.localize(datetime.utcnow())
        self.last_run = pytz.UTC.localize(datetime.utcfromtimestamp(os.stat(timestamp_file).st_mtime))
        log.info('Last run: %s' % self.last_run)
        
    def close(self):
        self.touch(self.timestamp_file, self.current_run)
            
    def touch(self, fname, times=None):
        if isinstance(times, datetime):
            time = (times - EPOCH).total_seconds()
            times = (time, time)
        
        with file(fname, 'a'):
            os.utime(fname, times)


class ElasticSearchMonitor(Monitor):
    jobs = None
    
    def __init__(self, index='inviso', doc_type='config', lookback_minutes=120,
                 checktime_minutes=60, elasticsearch=None, **kwargs):
        super(ElasticSearchMonitor, self).__init__(**kwargs)
        
        self.timestamp = arrow.utcnow();
        self.checktime = self.timestamp.replace(minutes=-checktime_minutes)
        
        if ElasticSearchMonitor.jobs is None:
            es = elasticsearch
            start = self.timestamp.replace(minutes=-lookback_minutes)
            stop = self.timestamp
                    
            body = '''
            {
                "query": {
                    "bool": {
                      "must": [
                        { "range": { "_timestamp": { "gte": %(start)s, "lte": %(stop)s }}}
                      ]
                    }
                }       
            }''' % {'start': start.timestamp*1000, 'stop':stop.timestamp*1000}
            
            r = es.search(index, doc_type, body, fields=['_id', '_timestamp'], size=10000)
            
            def gather(memo, value):
                memo[value['_id']] = value['fields']['_timestamp']
                return memo
            
            ElasticSearchMonitor.jobs = reduce(gather, r['hits']['hits'], {})

    def close(self):
        pass


class EmrMr1LogMonitor(TimestampMonitor):

    def __init__(self, bucket=None, log_path=None, **kwargs):
        super(EmrMr1LogMonitor, self).__init__(**kwargs)

        s3 = boto.connect_s3()
        self.emr_logs = Bucket(s3, bucket)

    def run(self):
        clusters = self.emr_logs.list(delimiter="/")

        threads = self.process_clusters(clusters)

        start = time.time()

        for thread in threads:
            thread.join()

        log.info('Completed in: %f seconds' % (time.time() - start))

    def process_clusters(self, clusters):
        threads = []

        for cluster in clusters:
            cluster_id = cluster.name
            cluster_name = re.sub('_([0-9]+)/?', '', cluster.name)

            jobflows = self.emr_logs.list(prefix=cluster.name, delimiter="/")

            def process_jobflow(cname, cid, jobflow):
                log.info('Processing flow: %s' % jobflow.name)
                config_paths = self.emr_logs.list(prefix=jobflow.name+"jobs/", delimiter="/")

                for config_path in config_paths:
                    ts = parse(config_path.last_modified)

                    if(ts <= self.last_run):
                        continue

                    job_id = job_pattern.match(config_path.name.split('/')[-1]).group(0)

                    log.debug("Publishing job event for: %s" % job_id)

                    self.publisher.publish([{
                                                'job.id': job_id,
                                                'job.type': 'mr1',
                                                'file.type': 'config' if config_path.name.endswith('_conf.xml') else 'history',
                                                'cluster.id': cid,
                                                'cluster.name': cname,
                                                'history.uri': 's3://' +config_path.bucket.name+'/' + config_path.key,
                                                'bucket': config_path.bucket.name,
                                                'key': config_path.key,
                                                'timestamp': str(ts)
                                            }])

                log.info('Flow complete: ' + jobflow.name)

            for jobflow in jobflows:
                t = Thread(target=process_jobflow, name=jobflow.name, args=[cluster_name, cluster_id, jobflow])
                t.start()
                threads.append(t)

        return threads


class S3Mr1LogMonitor(TimestampMonitor):

    def __init__(self, jobflow, cluster_id, cluster_name, bucket, prefix, **kwargs):
        super(S3Mr1LogMonitor, self).__init__(**kwargs)

        self.jobflow = jobflow
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.bucket = bucket
        self.prefix = prefix

        self.emr_logs = Bucket(boto.connect_s3(), bucket)

    def run(self):
        listing = self.emr_logs.list(prefix=self.prefix, delimiter="/")

        for f in listing:
            path = f.name

            if path.endswith('_conf.xml') or not path.split('/')[-1].startswith('job_'):
                continue

            ts = parse(f.last_modified)

            if(ts <= self.last_run):
                continue

            job_id = job_pattern.match(path.split('/')[-1]).group(0)

            config_path = path[:path.rfind('/')]+'/'+job_id+'_conf.xml'

            event = {
                'inviso.type': 'mr1',
                'job.id': job_id,
                'job.type': 'mr1',
                'file.type': ['history', 'config'],
                'jobflow' : self.jobflow,
                'cluster.id': self.cluster_id,
                'cluster': self.cluster_name,
                'history.uri': 's3://%s/%s' % (self.bucket,path),
                'config.uri':'s3://%s/%s' % (self.bucket,config_path),
                'bucket': self.bucket,
                'timestamp': str(ts),
                'epoch': int((ts - EPOCH).total_seconds()) * 1000,
                'mapreduce.version': 'mr1'
            }

            self.publisher.publish([event])


class S3Mr2LogMonitor(ElasticSearchMonitor):

    def __init__(self, jobflow, cluster_id, cluster_name, bucket, prefix, **kwargs):
        super(S3Mr2LogMonitor, self).__init__(**kwargs)

        self.jobflow = jobflow
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.bucket = bucket
        self.prefix = prefix

        self.emr_logs = Bucket(boto.connect_s3(), bucket)

    def run(self):
        listing = self.emr_logs.list(prefix=self.prefix, delimiter="/")

        for f in listing:
            path = f.name

            if not path.endswith('.jhist'):
                continue

            ts = arrow.get(f.last_modified)

            if(ts <= self.checktime):
                log.debug('Skipping old file: ' + f.name)
                continue

            job_id = job_pattern.match(path.split('/')[-1]).group(0)

            if job_id in self.jobs and self.jobs[job_id] >= ts.timestamp*1000:
                log.debug('Skipping processed file: ' + f.name)
                continue

            config_path = path[:path.rfind('/')]+'/'+job_id+'_conf.xml'

            event = {
                'inviso.type': 'mr2',
                'job.id': job_id,
                'application.id': job_id.replace('job_', 'application_'),
                'job.type': 'mr2',
                'file.type': ['history', 'config'],
                'jobflow' : self.jobflow,
                'cluster.id': self.cluster_id,
                'cluster': self.cluster_name,
                'history.uri': 's3://%s/%s' % (self.bucket,path),
                'config.uri':'s3://%s/%s' % (self.bucket,config_path),
                'bucket': self.bucket,
                'timestamp': str(ts),
                'epoch': ts.timestamp * 1000,
                'mapreduce.version': 'mr2'
            }

            log.info('Publishing event: (%s) %s ' % (event['cluster'], event['job.id']))
            self.publisher.publish([event])


class HdfsMr2LogMonitor(ElasticSearchMonitor):

    def __init__(self,
                 jobflow,
                 cluster_id,
                 cluster_name,
                 host='localhost',
                 port=9000,
                 log_path='/tmp/hadoop-yarn/staging/history/done', **kwargs):
        super(HdfsMr2LogMonitor, self).__init__(**kwargs)

        self.jobflow = jobflow
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.host = host
        self.port = port
        self.log_path = log_path

    def run(self):
        c = Client(self.host, self.port)

        listing = c.ls([self.log_path], recurse=True)

        for f in listing:
            path = f['path']

            if not path.endswith('.jhist'):
                continue

            ts = arrow.get(f['modification_time']/1000)

            if ts <= self.checktime:
                continue

            job_id = job_pattern.match(path.split('/')[-1]).group(0)

            if job_id in self.jobs and self.jobs[job_id] >= ts.timestamp*1000:
                log.debug('Skipping processed job: ' + job_id)
                continue

            config_path = path[:path.rfind('/')]+'/'+job_id+'_conf.xml'

            event = {
                'inviso.type': 'mr2',
                'job.id': job_id,
                'application.id': job_id.replace('job_', 'application_'),
                'job.type': 'mr2',
                'file.type': ['history', 'config'],
                'jobflow' : self.jobflow,
                'cluster.id': self.cluster_id,
                'cluster': self.cluster_name,
                'history.uri': 'hdfs://%s:%s%s' % (self.host,self.port,path),
                'config.uri':'hdfs://%s:%s%s' % (self.host,self.port,config_path),
                'host': self.host,
                'port': self.port,
                'timestamp': str(ts),
                'epoch': f['modification_time'],
                'mapreduce.version': 'mr2'
            }

            log.info('Publishing event: (%s) %s %s' % (event['cluster'], event['job.id'], ts))
            self.publisher.publish([event])
