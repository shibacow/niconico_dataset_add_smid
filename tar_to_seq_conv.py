#!/usr/bin/python
from subprocess import check_call
import threadpool
import time
import logging
from glob import glob
import os
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='myapp.log',
                    filemode='a')

srcdir='comment_temp'
dstdir='comment_seq_temp'
def conv_file(src):
    f=os.path.basename(src)
    f=f.split('.')[0]
    d=f+'_seq'
    dst=dstdir+os.sep+d
    opt='-Xrs -Xms=1024m -Xmx=1536m -XX:PermSize=128m -XX:MaxPermSize=128m -XX:NewSize=320m -XX:MaxNewSize=320m -XX:SurvivorRatio=2 -XX:TargetSurvivorRatio=80'
    opt='-client -Xms1024m -Xmx1024m'
    cmd='java %s -jar tar-to-seq.jar %s %s' % (opt,src,dst)
    logging.info('start:'+cmd)
    print cmd
    check_call(cmd,shell=True)
    logging.info('end:'+cmd)

def callback(*args,**keys):
    msg=str(args)+str(keys)
    logging.info(msg)
def main():
    pool=threadpool.ThreadPool(6)

    l=glob(srcdir+os.sep+'*.tar.gz')
    l=sorted(l)

    for f in l[:2]:
        conv_file(f)
    #args=range(1,10)
    #requests=threadpool.makeRequests(conv_file,args,callback)
    #[pool.putRequest(req) for req in requests]
    #pool.wait()
if __name__=='__main__':main()

