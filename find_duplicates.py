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
import yaml
import argparse
import itertools
import threading
import ast
from multiprocessing import Pool

max_threads = 8


class ImageGenerator:
    def __init__(self, cursor, db):
        self.cursor = cursor
        self.db = db

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        val = self.cursor.fetchone()
        if val:
            img = {'hash': val[0],
                   'signature': ast.literal_eval(val[1])
                   }

            paths = self.db.get_img_paths(img)
            img['path'] = paths
            return img
        else:
            raise StopIteration()


class DbMan:
    queries = None

    def __init__(self,
                 db='image_recognizer.db',
                 queries_file='queries.yaml'):
        if not DbMan.queries:
            with open(queries_file, 'r') as config:
                try:
                    DbMan.queries = yaml.load(config)
                except yaml.YAMLError as exc:
                    logging.error(exc)
                    raise exc
        self.db = db
        self.conn = {threading.current_thread(): sqlite3.connect(db)}
        with self.conn[threading.current_thread()] as conn:
            conn.execute(DbMan.queries['create_metadata_table'])
            conn.execute(DbMan.queries['create_paths_table'])

    def insert_images_data(self, images):
        if not self.conn[threading.current_thread()]:
            self.conn[threading.current_thread()] = sqlite3.connect(self.db)

        if type(images) is not list:
            images = [images]

        with self.conn[threading.current_thread()] as conn:
            signatures = [(image['hash'],
                           str(image['signature'])) for image in images]
            conn.executemany(DbMan.queries['insert_image_metadata'],
                             signatures)
            for image in images:
                paths = [(image['hash'], path) for path in
                         image['path']]
                conn.executemany(DbMan.queries['insert_path'],
                                 paths)
            logging.debug('Inserted {} in database'.format(signatures))

    def get_images_data(self, image_hashes):
        if not self.conn[threading.current_thread()]:
            self.conn[threading.current_thread()] = sqlite3.connect(self.db)

        if type(image_hashes) is not list:
            image_hashes = [image_hashes]

        with self.conn[threading.current_thread()] as conn:
            results = []
            for img_hash in image_hashes:
                res = conn.execute(DbMan.queries['get_image_metadata'],
                                   (img_hash,))
                data = res.fetchone()
                if data:
                    res = conn.execute(DbMan.queries['get_paths'],
                                       (img_hash,))

                    paths = res.fetchall()
                    if paths:
                        paths = [ast.literal_eval(path[0]) for path in paths]
                    else:
                        paths = []

                    results.append({'hash': data[0],
                                    'signature': ast.literal_eval(data[1]),
                                    'path': paths})
                    logging.debug('Extracted {} from database'.format(data))

    def get_all_images(self, page=0):
        if not self.conn[threading.current_thread()]:
            self.conn[threading.current_thread()] = sqlite3.connect(self.db)

        with self.conn[threading.current_thread()] as conn:
            res = conn.execute(DbMan.queries['get_all_images'])
            if res:
                logging.info('Extracted all images from database')
                return ImageGenerator(res, self)

    def check_images_presence(self, images):
        if not self.conn[threading.current_thread()]:
            self.conn[threading.current_thread()] = sqlite3.connect(self.db)

        if type(images) is not list:
            images = [images]

        non_present_images = []
        with self.conn[threading.current_thread()] as conn:
            for img in images:
                res = conn.execute(DbMan.queries['check_hash_existence'],
                                   (img['hash'],))

                if not res.fetchone():
                    non_present_images.append(img)

            return non_present_images

    def add_imgs_paths(self, img):
        if not self.conn[threading.current_thread()]:
            self.conn[threading.current_thread()] = sqlite3.connect(self.db)

        if type(img) is not list:
            img = [img]

        path_list = []
        for simg in img:
            path_list += [(simg['hash'], path) for path in simg['path']]

        with self.conn[threading.current_thread()] as conn:
            conn.executemany(DbMan.queries['insert_path'],
                             path_list)

    def clean_orphan_paths(self):
        if not self.conn[threading.current_thread()]:
            self.conn[threading.current_thread()] = sqlite3.connect(self.db)

        with self.conn[threading.current_thread()] as conn:
            res = conn.execute(DbMan.queries['get_all_paths'])
            for path in res:
                if not os.path.isfile(path[1]):
                    conn.execute(DbMan.queries['delete_path'], (path[0],))

    def get_img_paths(self, img):
        if not self.conn[threading.current_thread()]:
            self.conn[threading.current_thread()] = sqlite3.connect(self.db)

        with self.conn[threading.current_thread()] as conn:
            results = conn.execute(DbMan.queries['get_paths'],
                                   (img['hash'],))
            return [res[0] for res in results]


def md5sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(partial(f.read, 128), b''):
            d.update(buf)
    return d.hexdigest()


def calculate_file_signature(file_full_path, gis, db=DbMan()):
    file_full_path = str(file_full_path)
    file_hash = str(md5sum(file_full_path))

    if not db.check_images_presence({'hash': file_hash}):
        logging.info('Signature of {} already in database'
                     .format(file_full_path))
        try:
            db.add_imgs_paths({'hash': file_hash, 'path': [file_full_path]})
            logging.info('Added path {}'.format(file_full_path))
        except sqlite3.IntegrityError:
            pass
    else:
        try:
            image_signature = gis.generate_signature(file_full_path)
            img = {'signature': image_signature.tolist(),
                   'hash': file_hash,
                   'path': [file_full_path]
                   }
            db.insert_images_data(img)
            logging.debug('Calculated signature of {}: \n{}'
                          .format(file_full_path, image_signature))
        except Exception as ex:
            logging.error('An error occurred while processing {}'
                          .format(file_full_path))
            logging.error(ex)


def calculate_batch_signature(files_full_path):
    if type(files_full_path) is not list:
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


def calculate_signatures(root_path, db=DbMan()):
    gis = ImageSignature()

    db.clean_orphan_paths()

    with Pool(max_threads) as ppool:
        full_paths = []
        for path, subdirs, files in os.walk(root_path):
            for name in files:
                full_paths.append((PurePath(path, name), gis))

        ppool.starmap(calculate_file_signature, full_paths)


def match_images(img1, img2, threshold, gis, db=DbMan()):
    dis = gis.normalized_distance(np.array(img1['signature']),
                                  np.array(img2['signature']))

    if len(img1['path']) > 1:
        logging.info('Similar images found: {:.5f} \n{}\n'
                     .format(1, '\n'.join(img1['path'][1:])))

    if len(img2['path']) > 1:
        logging.info('Similar images found: {:.5f} \n{}\n'
                     .format(1, '\n'.join(img2['path'][1:])))

    if dis < threshold:
        logging.info('Similar images found: {:.5f} \n{}\n{}\n'
                     .format(dis,
                             img1['path'][0],
                             img2['path'][0]))


def find_matches(threshold, db=DbMan()):
    gis = ImageSignature()
    data = []

    logging.info('Image matching started')

    with Pool(max_threads) as ppool:
        for img1, img2 in itertools.combinations(db.get_all_images(), 2):
            data.append((img1, img2, threshold, gis))
            if len(data) > 100:
                ppool.starmap(match_images, data)
                data = []

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
