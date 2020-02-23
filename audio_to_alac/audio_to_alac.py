#!/usr/bin/env python3
import os
import subprocess
from functools import partial
from multiprocessing import Pool
from pprint import pprint

DEBUG = False
MAX_POOL_SIZE = 15

def get_roots_files_dirs(basedir, extension):
    rdfs = []
    for root, dirs, files in os.walk(basedir, topdown=False):
       obj = {"root": root, "files": [], "dirs": []}
       for name in files:
          if name.endswith(".{}".format(extension)):
              obj["files"].append(name)
       for name in dirs:
          obj["dirs"].append(name)
       rdfs.append(obj)
    return rdfs

def iter_rdfs(rdfs, basedir, targetdir, fformat, pool_size, shell=True):
    global DEBUG
    head, tail= os.path.split(basedir)
    if DEBUG:
        print("Head of basedir {}, tail of basedir {}".format(head, tail))
    for rdf in rdfs:
        root = rdf.get("root")
        dirs = rdf.get("dirs")
        files = rdf.get("files")
        cmds_to_run = []
        if None in (root, dirs, files):
            continue
        elif DEBUG:
            print("Working on:")
            pprint(rdf)
        fixed_root = root.replace(head, targetdir, 1)
        if DEBUG:
            print("Creating root dir: {}".format(fixed_root))
        os.makedirs(fixed_root, exist_ok=True)
        for directory in dirs:
            fixed_dir = os.path.join(fixed_root, directory)
            if DEBUG:
                print("Creating dir: {}".format(fixed_dir))
            os.makedirs(fixed_dir, exist_ok=True)
        for f in files:
            origin = os.path.join(root, f)
            new_file = f.replace(".{}".format(fformat), ".m4a")
            dest = os.path.join(fixed_root, new_file)
            print("Will convert: {} to: {}".format(origin, dest))
            cmd = get_convert_file_arguments(origin, dest, "alac")
            if shell:
                cmd = " ".join(cmd)
            cmds_to_run.append(cmd)
            #run_command(cmd, shell=shell)
        pool = Pool(pool_size)
        with pool:
            rc = partial(run_command, shell=shell)
            pool.map(rc, cmds_to_run)

def get_convert_file_arguments(origin, dest, codec):
    cmd = ["ffmpeg", "-i", '"{}"'.format(origin), "-acodec", codec, '"{}"'.format(dest)]
    return cmd

def run_command(cmd, shell=True):
    global DEBUG
    if DEBUG:
        print(cmd)
    try:
        return subprocess.run(cmd, shell=shell)
    except Exception as e:
        print(e)
        return None

def do_check_ffmpeg(shell=True):
    return subprocess.run(["ffmpeg"], shell=shell)

def main(**kwargs):
    global DEBUG
    DEBUG = kwargs.get("DEBUG")
    basedir = kwargs.get("basedir")
    targetdir = kwargs.get("targetdir")
    fformat = kwargs.get("fformat")
    check_ffmpeg = kwargs.get("check_ffmpeg")
    shell = not kwargs.get("no_shell")
    pool_size = min(kwargs.get("pool_size"), MAX_POOL_SIZE)
    if check_ffmpeg:
        result = do_check_ffmpeg(shell=shell)
        if result is None:
            print("Error running ffmpeg")
        return
    if DEBUG:
        print("Running with pool size {}".format(pool_size))
    rdfs = get_roots_files_dirs(basedir, fformat)
    iter_rdfs(rdfs, basedir, targetdir, fformat, pool_size, shell=shell)
            
    

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Audio to ALAC")
    parser.add_argument('--base', metavar='BASE DIR', type=str, dest='basedir', help='Base directory to search in', required=True)
    parser.add_argument('--target', metavar='TARGET DIR', type=str, dest='targetdir', help='Target directory to put files in', required=True)
    parser.add_argument('--format', metavar='FILE FORMAT', type=str, dest='fformat', help='File format to search', required=True)
    parser.add_argument('--debug', action='store_true', dest='DEBUG', help='Debug mode')
    parser.add_argument('--check-ffmpeg', action='store_true', dest='check_ffmpeg', help='Checks if ffmpeg is installed')
    parser.add_argument('--no-shell', action='store_true', dest='no_shell', help='Disable subprocess shell mode')
    parser.add_argument('--pool-size', metavar='POOL SIZE', type=int, dest='pool_size', help='Process Pool size for threads', default=5)
    args = parser.parse_args()
    arg_dict = vars(args)
    main(**arg_dict)

