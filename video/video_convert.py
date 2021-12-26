#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import zipfile
import gzip
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures as confu
from glob import glob
import json
import re
import logging
logging.basicConfig(level=logging.INFO)
src='../video/*.zip'
dstdir='../video_dst/'
def read_zip_file(f):
    with zipfile.ZipFile(f) as zp:
        for info in zp.infolist():
            with zp.open(info,'r') as zfile:
                for l in zfile.readlines():
                    l=l.strip()
                    try:
                        yield json.loads(l)
                    except json.decoder.JSONDecodeError as err:
                        logging.error("f={} info={} l={} err={}".format(f,info,l,err))
def conv_zip_file(f,i,sz):
    dst=re.findall('([\d]+)\.zip',f)
    dstf=dstdir+dst[0]+'.json.gz'
    if i%100==0:
        logging.info("start sz={} i={}  {}".format(sz,i,dstf))
    with gzip.GzipFile(dstf,'w') as gf:
        for d in read_zip_file(f):
            l=json.dumps(d)
            l=l+'\n'
            l=l.encode('utf-8')
            gf.write(l)
    if i%100==0:
        logging.info("end sz={} i={} {}".format(sz,i,dstf))
def main():
    l=sorted(list(glob(src)))
    sz=len(l)
    with ProcessPoolExecutor() as executor:
        futures=[executor.submit(conv_zip_file,f,i,sz) for i,f in enumerate(l)]
        for future in confu.as_completed(futures):
            if future.result():
                logging.info(u"r={}".format(future.result()))

if __name__=='__main__':main()

    
