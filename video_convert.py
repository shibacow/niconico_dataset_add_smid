#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import zipfile
import gzip
from glob import glob
import json
import re
src='../video/*.zip'
dstdir='../video_dst/'
def read_zip_file(f):
    with zipfile.ZipFile(f) as zp:
        for info in zp.infolist():
            with zp.open(info,'r') as zfile:
                for l in zfile.readlines():
                    l=l.strip()
                    yield json.loads(l)
                    
def fconv_zip_file(f):
    dst=re.findall('([\d]+)\.zip',f)
    dstf=dstdir+dst[0]+'.json.gz'
    with gzip.GipFile(dstf,'wt') as gf:
        for d in read_zip_file(f):
            l=json.dumps(d)
            l=l+'\n'
            gf.write(l)
            
def main():
    l=list(glob(src))[:1]
    for c in l:
        conv_zip_file(c)
if __name__=='__main__':main()

    
