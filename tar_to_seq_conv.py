#!/usr/bin/python
from subprocess import check_call
import threadpool
import time
import tarfile
import logging
from glob import glob
import os
import shutil
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='myapp.log',
                    filemode='a')

srcdir='comment_src'
dstdir='comment_seq'

def tar_extract(src):
    tar=tarfile.open(src)
    tar.extractall(srcdir)
def conv_file(src):
    start=time.clock()
    tar_extract(src)
    f=os.path.basename(src)
    f=f.split('.')[0]
    d=f+'_seq'
    dst=dstdir+os.sep+d
    ssrcdir=src.split('.')[0]
    opt='-Xrs -Xms=1024m -Xmx=1536m -XX:PermSize=128m -XX:MaxPermSize=128m -XX:NewSize=320m -XX:MaxNewSize=320m -XX:SurvivorRatio=2 -XX:TargetSurvivorRatio=80'
    opt='-client -Xms1024m -Xmx1024m'
    cmd='java %s -jar tar-to-seq.jar %s %s' % (opt,ssrcdir,dst)
    logging.info('start:'+cmd)
    print cmd
    check_call(cmd,shell=True)
    logging.info('end:'+cmd)
    shutil.rmtree(ssrcdir)
    end=time.clock()-start
    print "spent time=%f cmd=%s" % (end,cmd)

def callback(*args,**keys):
    msg=str(args)+str(keys)
    logging.info(msg)
def main():
    pool=threadpool.ThreadPool(6)
    l=glob(srcdir+os.sep+'*.tar.gz')
    args=sorted(l)
    requests=threadpool.makeRequests(conv_file,args,callback)
    [pool.putRequest(req) for req in requests]
    pool.wait()
if __name__=='__main__':main()

