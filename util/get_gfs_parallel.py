#!/usr/bin/python3
from datetime import datetime
import wget
import sys
import argparse
import shutil
import urllib.request as request
from contextlib import closing
from urllib.error import URLError
import time
import glob


def get_runs(date_string):
    url = 'ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.'+date_string+'/'
    try:
        with closing(request.urlopen(url)) as r:
            with open('avail_runs', 'wb') as f:
                shutil.copyfileobj(r, f)
    except URLError as e:
        if e.reason.find('No such file or directory') >= 0:
            raise Exception('FileNotFound')
        else:
            raise Exception(f'Something else happened. "{e.reason}"')
    avail = open("avail_runs", "r").read()
    avail = avail.splitlines()
    avail = [x[-2:] for x in avail]
    return avail


def get_files(date_string,run):
    url = 'ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.'+date_string+'/'+str(run)+'/atmos/'
    try:
        with closing(request.urlopen(url)) as r:
            with open('avail_files', 'wb') as f:
                shutil.copyfileobj(r, f)
    except URLError as e:
        if e.reason.find('No such file or directory') >= 0:
            raise Exception('FileNotFound')
        else:
            raise Exception(f'Something else happened. "{e.reason}"')
    avail = open("avail_files", "r").read()
    avail = avail.splitlines()
    return avail


def is_it_runtime(date_string,run):
    while True:
        print('get runs')
        available_runs = get_runs(date_string)
        print('check')
        if str(run) in available_runs: break
        print('sleep')
        time.sleep(1200)
    return "YES"


def pullable_files(date_string,run,names):  # mod this to return
    available=[]
    available_files=get_files(date_string,run)
    for name in names:
        check = [i for i in  [x.find(name) for x in available_files] if i >= 0]
        if len(check)>0:
            available.append(name)
    return available



def run_wget(link,file):
    wget.download(link,out = file)
    return file


def try_again(down, sleep, file_dest):
        still_need_files = [x for x in file_dest if x not in down]
        still_need_links = [link+x.split('/')[-1] for x in down]
        from multiprocessing import Pool
        pool = Pool(processes=15)
        pooled = [pool.apply_async(run_wget, args=(link,file)) for link, file in zip(still_need_links,still_need_files)]
        time.sleep(sleep*len(still_need_files))


def get_downloaded(data_dir):
    return [down for down in glob.glob(data_dir+"*") if down[-1] != 'p']
