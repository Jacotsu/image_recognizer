#!/usr/bin/env python3

from image_match.goldberg import ImageSignature
import hashlib
import numpy as np
import sqlite3
from functools import partial
from pathlib import PurePath
import os
import sys
import logging
import json
import yaml
import argparse
import itertools
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from concurrent.futures import ALL_COMPLETED

max_threads = 8


class DbMan:
    queries = None

    def __init__(self,
                 db='image_recognizer.db',
                 queries_file='queries.sql'):
        if not DbMan.queries:
            with open(queries_file, 'r') as config:
                try:
                    DbMan.queries = yaml.load(config)
                except yaml.YAMLError as exc:
                    logging.error(exc)
                    raise exc

        self.conn = sqlite3.connect(db)
        with self.conn:
            self.conn.execute(DbMan.queries['create_tables'])

    def insert_images_data(self, image_tuples):
        if image_tuples is not list:
            image_tuples = [image_tuples]

        with self.conn:
            self.con.executemany(DbMan.queries['insert_image_metadata'],
                                 image_tuples)

    def get_images_data(self, image_hashes):
        if image_hashes is not list:
            image_hashes = [image_hashes]


def md5sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(partial(f.read, 128), b''):
            d.update(buf)
    return d.hexdigest()


def calculate_file_signature(file_full_path, gis, db):
    file_hash = str(md5sum(file_full_path))

    if file_hash in db.keys():
        logging.debug('Signature of {} already in database'
                      .format(file_full_path))
        return
    try:
        image_signature = gis.generate_signature(file_full_path)
        db[file_hash] = {'signature': image_signature.tolist(),
                         'path': file_full_path
                         }
        logging.debug('Calculated signature of {}: \n{}'
                      .format(file_full_path, image_signature))
    except:
        logging.error('An error occurred while processing {}'
                      .format(file_full_path))


def calculate_batch_signature(files_full_path):
    if files_full_path is not list:
        files_full_path = [files_full_path]

    gis = ImageSignature()

    signatures = []

    for file_full_path in files_full_path:
        file_hash = str(md5sum(file_full_path))

        try:
            image_signature = gis.generate_signature(file_full_path)
            signatures.append((image_signature.tolist(),
                               file_hash,
                               file_full_path
                               ))
            logging.debug('Calculated signature of {}: \n{}'
                          .format(file_full_path, image_signature))
        except:
            logging.error('An error occurred while processing {}'
                          .format(file_full_path))
    return signatures


def calculate_signatures(root_path, database='signatures.json'):
    executor = ThreadPoolExecutor(max_workers=max_threads)
    fs = []

    gis = ImageSignature()
    db = {}

    try:
        with open(database, 'r') as data:
            db = json.load(data)
    except FileNotFoundError:
        pass

    for path, subdirs, files in os.walk(root_path):
        for i, name in enumerate(files):
            file_full_path = str(PurePath(path, name))

            fs.append(executor.submit(calculate_file_signature,
                                      file_full_path,
                                      gis,
                                      db)
                      )
            if (i % max_threads == 0):
                wait(fs, timeout=None, return_when=ALL_COMPLETED)
                fs = []

    with open(database, 'w') as data:
        json.dump(db, data)


def match_images(img1, img2, threshold, db, gis):
    dis = gis.normalized_distance(np.array(db[img1]['signature']),
                                  np.array(db[img2]['signature']))
    if dis < threshold:
        logging.info('Similar images found: {:.5f} \n {} \n {} \n'
                     .format(dis,
                             db[img1]['path'],
                             db[img2]['path']))


def find_matches(threshold, database='signatures.json'):
    executor = ThreadPoolExecutor(max_workers=max_threads)
    fs = []

    gis = ImageSignature()
    db = {}

    try:
        with open(database, 'r') as data:
            db = json.load(data)
    except FileNotFoundError:
        logging.error("Database not found, run find_duplicates update first!")
        return

    i = 0
    for img1, img2 in itertools.combinations(db.keys(), 2):
        i += 1
        fs.append(executor.submit(match_images,
                                  img1,
                                  img2,
                                  threshold,
                                  db,
                                  gis
                                  )
                  )
        if (i % max_threads == 0):
            wait(fs, timeout=None, return_when=ALL_COMPLETED)
            fs = []

    logging.info('Image matching finished')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('command',
                        help='update \t Updates the signatures database \n'
                             'match \t Finds similar images in the directory',
                        default='match')
    parser.add_argument('path', help='Specifies the working path',
                        default='.')
    parser.add_argument("-t", "--threshold",
                        help='Set image difference threshold',
                        default=.3)
    args = parser.parse_args()

    logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
    logging.getLogger().addHandler(logging.FileHandler('logfile.log'))
    logging.getLogger().setLevel(logging.INFO)

    if args.command == 'update':
        calculate_signatures(args.path)
    else:
        find_matches(args.threshold)
