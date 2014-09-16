import boto


class SQSBase(object):
    def __init__(self, region="us-east-1", **kwargs):
        self.conn = boto.sqs.connect_to_region(region)

