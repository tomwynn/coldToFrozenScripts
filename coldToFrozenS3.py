#!/usr/bin/env python3
import sys, os, gzip, shutil, subprocess, random
import logging
import urllib.request
import datetime
import time
import tarfile
import glob
import subprocess
from logging.handlers import RotatingFileHandler

#Define Archive Directory
ARCHIVE_DIR = '/data/splunkWarm/frozenData'

#Define Host Name
hostname=os.uname()[1]

class ColdToFrozenS3Error(Exception):
    pass

#Set up logging
log_file_path = '/opt/splunk/var/log/splunk/splunkArchive.log'
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
#logger.info('Started on '+str(datetime.datetime.today()))
#logger.info("TEST")

def handleNewBucket(base, files):
    print('Archiving bucket: ' + base)
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
                except (OSError, tarfile.TarError) as e:
                        logger.error('Error: tar archive creation failed' + str(e))
                else:
                        shutil.rmtree(source_dir)
                        logger.info(os.path.basename(source_dir)+' was removed')
 
if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit('usage: python coldToFrozenExample.py <bucket_dir_to_archive>')

    if not os.path.isdir(ARCHIVE_DIR):
        try:
            os.mkdir(ARCHIVE_DIR)
        except OSError:
            # Ignore already exists errors, another concurrent invokation may have already created this dir
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
        handleNewBucket(bucket, files)
    else:
        sys.exit('No journal file found, bucket invalid:' + bucket)

    if bucket.endswith('/'):
        bucket = bucket[:-1]

    indexname = os.path.basename(os.path.dirname(os.path.dirname(bucket)))
    destdir = os.path.join(ARCHIVE_DIR, indexname, os.path.basename(bucket))
    container_name = indexname
    full_index_path=os.path.join(ARCHIVE_DIR,indexname)

    s3bucket = 'splunkarchive'
    instance_id = urllib.request.urlopen('http://169.254.169.254/latest/meta-data/instance-id').read().decode()
    print('instance ID:', instance_id)
    remotepath = 's3://{s3bucket}/{instanceid}/{index}/'.format(
        s3bucket=s3bucket,
        instanceid=instance_id,
        index=indexname) 

    while os.path.isdir(destdir):
        print('Warning: This bucket already exists in the archive directory')
        print('Adding a random extension to this directory...')
        destdir += '.' + str(random.randrange(10))

    shutil.copytree(bucket, destdir)

    for bucket_dir in os.listdir(full_index_path):
        bucket_path=os.path.join(full_index_path,bucket_dir)
        if os.path.isdir(bucket_path):
            output_filename=full_index_path+'/'+hostname+'_'+indexname+'_'+bucket_dir+'.tar.gz'
            make_index_bucket_tarfile(output_filename,bucket_path)

    for files in os.listdir(full_index_path):
                logger.info('files: '+files)
                file_path=os.path.join(full_index_path,files)
                logger.info('file_path: '+file_path)
                if files.endswith((".gz")):
                        s3args = 'cp ' + file_path + ' ' + remotepath
                        command = '/usr/local/bin/aws s3 ' + s3args
                        command = command.split(' ')
                        #logger.info('command: '+command)
                        try:
                            awscli = subprocess.check_call(
                            command,
			                stdout=sys.stdout,
			                stderr=sys.stderr,
			                timeout=900)
                            #os.remove(file_path)
                        except subprocess.TimeoutExpired:
                            raise ColdToFrozenS3Error("S3 upload timedout and was killed")
                        except:
                            raise ColdToFrozenS3Error("Failed executing AWS CLI")
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                        print('Froze {0} OK'.format(sys.argv[1]))
