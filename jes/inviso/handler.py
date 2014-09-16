import json
from boto import sqs
from aws import SQSBase
from base64 import b64decode
import xml.etree.ElementTree as ET
import gzip
import cStringIO as StringIO
from urlparse import urlparse

from snakebite.client import Client
from elasticsearch.helpers import bulk

import boto
from boto.s3.bucket import Bucket
from concurrent.futures import ThreadPoolExecutor
from elasticsearch import Elasticsearch
from .util import get_logger


log = get_logger('inviso-handler')


def make_batch(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


class EventHandler(object):
    def __init__(self, **kwargs):
        pass
        
    def handle(self, events):
        raise NotImplementedError("Clients implement this method")


class SQSEventHandler(EventHandler, SQSBase):
    def __init__(self, queue=None, max_receive=40, timeout=60, **kwargs):
        EventHandler.__init__(self, **kwargs)
        SQSBase.__init__(self, **kwargs)
    
        self.queue = self.conn.get_queue(queue)
        self.queue.set_message_class(sqs.message.Message)
        self.max_receive = max_receive
        self.timeout = timeout
        self.event_map = {}
        self.complete_messages = []

    def poll(self):
        while True:
            self.complete_messages = []
            self.event_map = {}

            messages = self.get_messages()
            
            if not messages:
                log.info('No messages remaining.')
                break
            
            log.info('Processing %s messages' % len(messages))
            
            events = []
            
            for message in messages:
                try:
                    event = json.loads(message.get_body())
                    events.append(event)

                    self.event_map[event['job.id']] = message
                except Exception as e:
                    print e.message
                
            self.handle(events)

            if self.complete_messages:
                for batch in make_batch(self.complete_messages, 10):
                    self.queue.delete_message_batch(batch)

    def mark_complete(self, job_id):
        self.complete_messages.append(self.event_map[job_id])

    def get_messages(self):
        
        messages = []
        fetch = min(self.max_receive, 10)
        
        while True:
            batch = self.queue.get_messages(num_messages=fetch, visibility_timeout=self.timeout)
            
            if len(batch) == 0:
                return messages
            
            messages.extend(batch)
            
            if len(messages) >= self.max_receive:
                return messages
            
            fetch = min(self.max_receive - len(messages), 10)

    def handle(self, events):
        raise NotImplementedError("Clients implement this method")


class IndexHandler(EventHandler):
    def __init__(self, elasticsearch=None, index='inviso', trace=None, genie=None, archiver=None, **kwargs):
        super(IndexHandler, self).__init__(**kwargs)

        if not elasticsearch:
            elasticsearch = Elasticsearch({'host': 'localhost', 'port': 9200})

        self.elasticsearch = elasticsearch
        self.index = index
        self.trace = trace
        self.genie = genie
        self.archiver = archiver

    def handle(self, events):
        log.info('Processing %s events' % len(events))

        futures = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            for event in events:
                if self.archiver:
                    executor.submit(self.archiver.archive, event)
                futures.append(executor.submit(self.process_event, event))

        documents = [future.result() for future in futures if future.result() is not None]

        self._update_index(documents)
        log.info('Events complete.')

    def process_event(self, event):
        try:
            doc = {}
            doc.update(event)

            self._process_config(doc, event)
            self._process_history(doc, event)
            self._process_genie_info(doc, event)

            index_doc = {
                '_op_type': 'index',
                '_index': 'inviso',
                '_type': 'config',
                '_id': event['job.id'],
                '_ttl': '90d',
                '_timestamp': event['epoch'],
                '_source': doc
            }

            return index_doc
        except Exception as e:
            log.exception(e)

        return None

    def close(self):
        self.elasticsearch.indices.refresh(index=self.index)

    def fetch_content(self, uri):
        p = urlparse(uri)

        if p.scheme == 'hdfs':
            host, port = p.netloc.split(':')

            c = Client(host, int(port))

            content = StringIO.StringIO()

            for line in c.text([p.path]):
                content.write(line)

            return content.getvalue()

        if p.scheme == 's3':
            bucket = Bucket(self.s3, p.netloc)
            key = bucket.get_key(key_name=p.path[1:])

            return key.get_contents_as_string()

    def _process_config(self, doc, event):
        content = self.fetch_content(event['config.uri'])

        root = ET.fromstring(content)

        for p in root.findall('property'):
            name = p.findtext('name')
            value = p.findtext('value')

            if name == 'pig.script':
                value = b64decode(value)

            doc[name] = value

    def _process_genie_info(self, doc, event):
        if self.genie is None or 'genie.job.id' not in doc:
            return

        info = genie_lookup(doc)

        if 'jobInfo' in info.get('jobs',{}):
            info = info['jobs']['jobInfo']
            doc.update({
                'genie.job.name': info.get('jobName', None),
                'genie.start': info.get('startTime', None),
                'genie.stop': info.get('finishTime', None),
                'genie.status': info.get('status', None),
                'genie.user': info.get('userName', None),
                'genie.schedule': info.get('schedule', None),
                'genie.job.type': info.get('jobType', None)
            })

    def _process_history(self, doc, event):
        job_id = event['job.id']
        uri = event['history.uri']
        version = event['inviso.type'] or 'mr1'

        trace = self.trace(job_id, uri, version)

        if not trace:
            log.warn('No trace info available for %s' % uri)
            return

        if version == 'mr2':
            doc.update({
                'mapred.job.submit': trace.get('submitTime', 0),
                'mapred.job.start': trace.get('launchTime', 0),
                'mapred.job.stop': trace.get('finishTime', 0),
                'mapred.job.status': trace.get('jobStatus', 'unknown'),
                })
        elif version == 'mr1':
            doc.update({
                'mapred.job.submit': trace.get('submit_time',0),
                'mapred.job.start': trace.get('launch_time',0),
                'mapred.job.stop': trace.get('finish_time',0),
                'mapred.job.status': trace.get('status', 'unknown'),
                })
        else:
            log.warn('Unknown version: ' + version)

    def _update_index(self, documents):
        log.info("Indexing %s documents" % (len(documents)) )
        response = bulk(self.elasticsearch, documents)
        log.debug(response)


class S3Archiver():
    def __init__(self, bucket, path='history', hash_depth=3):
        self.s3 = boto.connect_s3()
        self.inviso_store = Bucket(self.s3, bucket)
        self.history_path = path
        self.hash_depth = hash_depth

    def archive(self, event):
        job_id = event['job.id']
        dest_name = job_id

        prefix = dest_name[-self.hash_depth:] + '/'

        for uri, suffix in [(event['history.uri'], '.history.gz'), (event['config.uri'], '.conf.gz')]:
            content = self.fetch_content(uri)

            gz_out = StringIO.StringIO()

            with gzip.GzipFile(fileobj=gz_out, mode="w") as gz:
                gz.write(content)

            new_key = self.inviso_store.new_key(self.history_path+prefix+dest_name+suffix)
            new_key.set_contents_from_string(gz_out.getvalue())

        log.debug("Transfer complete: " + dest_name)


class IndexArchiveHandlerMr1(IndexHandler, SQSEventHandler):
    def __init__(self, **kwargs):
        super(IndexHandler, self).__init__(**kwargs)
        super(IndexArchiveHandlerMr1, self).__init__(**kwargs)

    def process_event(self, event):
        try:
            doc = {}
            doc.update(event)

            self._process_config(doc, event)
            self._process_history(doc, event)
            self._process_genie_info(doc, event)

            index_doc = {
                '_op_type': 'index',
                '_index': 'inviso',
                '_type': 'config',
                '_id': event['job.id'],
                '_ttl': '90d',
                '_timestamp': event['epoch'],
                '_source': doc
            }

            self.mark_complete(event['job.id'])
            return index_doc
        except Exception as e:
            log.exception(e)

        return None


class IndexArchiveHandlerMr2(SQSEventHandler):
    def __init__(self, **kwargs):
        super(IndexHandler, self).__init__(**kwargs)
        super(IndexArchiveHandlerMr2, self).__init__(**kwargs)

    def process_event(self, event):
        try:
            doc = {}
            doc.update(event)

            self._process_config(doc, event)
            self._process_history(doc, event)
            self._process_genie_info(doc, event)

            index_doc = {
                '_op_type': 'index',
                '_index': self.index,
                '_type': 'config',
                '_id': event['job.id'],
                '_ttl': '90d',
                '_timestamp': event['epoch'],
                '_source': doc
            }

            self.mark_complete(event['job.id'])
            return index_doc
        except Exception as e:
            log.warn('Failed to process event: %s' % (event,))
            log.exception(e)

        return None





