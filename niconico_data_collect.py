#!/usr/bin/python
# -*- coding:utf-8 -*-
import requests
from pyquery import PyQuery as pq
from urlparse import urljoin
import os
import urllib2
def video_meta_data_fetch():
    src='http://tcserv.nii.ac.jp/access/shibacow@gmail.com/b0cd188dc03daa68/nicocomm/data/video/video.html'
    d=pq(url=src)
    
    for i, a in enumerate(d('a')):
        aa=pq(a)
        fname=aa.attr.href
        fsk=urljoin(src,fname)
        dst=open('meta_data'+os.sep+fname,'wb')
        r2=urllib2.urlopen(fsk)
        dst.write(r2.read())
        print i,fname

def comment_meta_data_fetch():
    src='http://tcserv.nii.ac.jp/access/shibacow@gmail.com/b0cd188dc03daa68/nicocomm/data/thread/thread.html'
    d=pq(url=src)
    
    for i, a in enumerate(d('a')):
        aa=pq(a)
        fname=aa.attr.href
        fsk=urljoin(src,fname)
        dst=open('comment_data'+os.sep+fname,'wb')
        r2=urllib2.urlopen(fsk)
        dst.write(r2.read())
        print i,fname

def main():
    #video_meta_data_fetch()
    comment_meta_data_fetch()
if __name__=='__main__':main()
