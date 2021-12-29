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
import os
logging.basicConfig(level=logging.INFO)
src='video/*.jsonl.gz'
dstdir='video_dst'
if not os.path.isdir(dstdir):
    os.mkdir(dstdir)
def read_gz_file(f):
    with gzip.GzipFile(f) as gzf:
        for l in gzf.readlines():
            l=l.strip()
            try:
                yield json.loads(l)
            except json.decoder.JSONDecodeError as err:
                logging.error("f={} info={} l={} err={}".format(f,info,l,err))
def conv_gz_file(f,i,sz):
    dst=re.findall('([\d]+)\.jsonl.gz',f)
    dstf=dstdir+os.sep+dst[0]+'.jsonl.gz'
    if i%100==0:
        logging.info("start sz={} i={}  {}".format(sz,i,dstf))
    with gzip.GzipFile(dstf,'w') as gf:
        for d in read_gz_file(f):
            d['tags']=d['tags'].split(' ')
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
        futures=[executor.submit(conv_gz_file,f,i,sz) for i,f in enumerate(l)]
        for future in confu.as_completed(futures):
            if future.result():
                logging.info(u"r={}".format(future.result()))

if __name__=='__main__':main()

    
