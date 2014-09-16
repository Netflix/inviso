import sys
import settings_default
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

from handler import IndexArchiveHandlerMr2
from inviso import util


log = util.get_logger('index-archive')

def main():
    """Command line options."""
    # Setup argument parser
    parser = ArgumentParser(description='Index/Archive for MR2', formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-q", "--queue", dest="queue", action="store", help="Event queue name.")
    parser.add_argument("-b", "--bucket", dest="bucket", action="store", help="Bucket to store archived job files.")
    
    # Process arguments
    args = parser.parse_args()
    queue = args.queue
    bucket = args.bucket
    
    ia = IndexArchiveHandlerMr2(
        queue=queue,
        inviso_bucket=bucket,

    )

    ia.poll()
    ia.close()        
        
if __name__ == "__main__":
    sys.exit(main())        
