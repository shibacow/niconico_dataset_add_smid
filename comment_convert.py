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
fmt = "%(asctime)s %(levelname)s %(name)s :%(message)s"
logging.basicConfig(level=logging.INFO,format=fmt)
src='../comment/*.zip'
dstdir='../comment_dst/'
def read_zip_file(f):
    with zipfile.ZipFile(f) as zp:
        for info in zp.infolist():
            if info.is_dir():continue
            try:
                with zp.open(info,'r') as zfile:
                    zinfo=re.findall('\/(.+)\.jsonl',info.filename)[0]
                    for l in zfile.readlines():
                        l=l.strip()
                        try:
                            d=json.loads(l)
                            d['video_id']=zinfo
                            yield d
                        except json.decoder.JSONDecodeError as err:
                             logging.error("f={} info={} l={} err={}".format(f,info,l,err))
            except zipfile.BadZipFile as err:
                logging.error("f={} info={} err={}".format(f,info,err))
                            
def conv_zip_file(f,i,sz):
    dst=re.findall('([\d]+)\.zip',f)
    dstf=dstdir+dst[0]+'.json.gz'
    if i%50==0:
        logging.info("start sz={} i={}  {}".format(sz,i,dstf))
    with gzip.GzipFile(dstf,'w') as gf:
        ls=[]
        for d in read_zip_file(f):
            l=json.dumps(d)
            l=l+'\n'
            l=l.encode('utf-8')
            ls.append(l)
            if len(ls) > 1000:
                gf.writelines(ls)
                ls=[]
        if ls:
            gf.writelines(ls)
    if i%50==0:
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

    
