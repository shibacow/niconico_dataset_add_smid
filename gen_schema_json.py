#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import json
def main():
    jd=[]
    for l in open('comment_schema_base.txt','rt').readlines():
        l=l.strip()
        l=[k.strip() for k in l.split('\t')]
        dkt=dict(name=l[0],type=l[1],mode=l[2])
        jd.append(dkt)
    with open('comment.json','wt') as outf:
        json.dump(jd,outf)
if __name__=='__main__':main()

