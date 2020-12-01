import sys, os, gzip, shutil, subprocess, random
import logging
import urllib
import urllib2
import requests, json
import datetime
import time
import tarfile
import glob
from logging.handlers import RotatingFileHandler
from azure.storage.blob import BlockBlobService, PublicAccess

#Define Archive Directory
ARCHIVE_DIR = '/Splunk/frozenData'

#Define Host Name
hostname=os.uname()[1]

#Set up logging
log_file_path = '/home/splunk/splunkArchiveLog/splunkArchive.log'
app_name = 'SplunkArchive'
def get_module_logger(app_name,file_path):
        logger = logging.getLogger(app_name)
        fh = logging.FileHandler(file_path)
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - process=%(name)s - status=%(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.setLevel(logging.INFO)
        return logger

#Define time
today=round(time.mktime(datetime.datetime.today().timetuple()))
one_month_earlier=today-120*86400

#Start Logger
logger = get_module_logger(app_name='SplunkArchive',file_path=log_file_path)
logger.info('Started on '+str(datetime.datetime.today()))
#logger.info("TEST")

#Archive Buckets
def archiveBucket(base, files):
        print 'Archiving bucket: ' + base
        for f in files:
                full = os.path.join(base, f)
                if os.path.isfile(full):
                        os.remove(full)

#Tar Archived Buckets
def make_index_bucket_tarfile(output_filename,source_dir):
        with tarfile.open(output_filename, "w:gz") as tar:
                try:
                        tar.add(source_dir,arcname=os.path.basename(source_dir))
                        logger.info(output_filename+' was created')
                except (OSError, tarfile.TarError), e:
                        logger.error('Error: tar archive creation failed')
                else:
                        shutil.rmtree(source_dir)
					   logger.info(os.path.basename(source_dir)+' was removed')

if __name__ == "__main__":
        if len(sys.argv) != 2:
                sys.exit('usage: python cold2frozen.py <bucket_path>')
        if not os.path.isdir(ARCHIVE_DIR):
                try:
                        os.mkdir(ARCHIVE_DIR)
                except OSError:
                        sys.stderr.write("mkdir warning: Directory '" + ARCHIVE_DIR + "' already exists\n")
        bucket = sys.argv[1]
        if not os.path.isdir(bucket):
                sys.exit('Given bucket is not a valid directory: ' + bucket)
        rawdatadir = os.path.join(bucket, 'rawdata')
        if not os.path.isdir(rawdatadir):
                sys.exit('No rawdata directory, given bucket is likely invalid: ' + bucket)
        files = os.listdir(bucket)
        journal = os.path.join(rawdatadir, 'journal.gz')
        if os.path.isfile(journal):
                archiveBucket(bucket, files)
        else:
                sys.exit('No journal file found, bucket invalid:' + bucket)

        if bucket.endswith('/'):
                bucket = bucket[:-1]


	    indexname = os.path.basename(os.path.dirname(os.path.dirname(bucket)))
        destdir = os.path.join(ARCHIVE_DIR,indexname,os.path.basename(bucket))
        container_name = indexname


        block_blob_service = BlockBlobService(account_name='test', account_key='test')
        block_blob_service.create_container(indexname)

        full_index_path=os.path.join(ARCHIVE_DIR,indexname)

        blobs_generator = block_blob_service.list_blobs(container_name)
        logger.info("blobs_generator %s" % blobs_generator)
        for bucket_dir in os.listdir(full_index_path):
                bucket_path=os.path.join(full_index_path,bucket_dir)
                if os.path.isdir(bucket_path):
                        logger.info('Working on the bucket '+bucket_path)
                        logger.info('The Bucket is older than 120 days')
                        output_filename=full_index_path+'/'+hostname+'_'+indexname+'_'+bucket_dir+'.tar.gz'
                        make_index_bucket_tarfile(output_filename,bucket_path)
        for files in os.listdir(full_index_path):
                file_path=os.path.join(full_index_path,files)
                if files.endswith((".gz")):
                        block_blob_service.create_blob_from_path(container_name, files, file_path)
                        os.remove(file_path)
        while os.path.isdir(destdir):
                print 'Warning: This bucket already exists in the archive directory'
                print 'Adding a random extension to this directory...'
                destdir += '.' + str(random.randrange(10))
        shutil.copytree(bucket, destdir)
