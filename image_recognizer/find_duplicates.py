import hashlib
import numpy as np
import sqlite3
import os
import sys
import logging
import argparse
import itertools
import threading
import ast
from multiprocessing import Pool
from functools import partial
from pathlib import PurePath
from image_match.goldberg import ImageSignature
from appdirs import user_data_dir
from image_recognizer import queries

max_threads = 8
appname = 'image_recognizer'


def chunks(l, n):
    """
    https://stackoverflow.com/questions/312443
    /how-do-you-split-a-list-into-evenly-sized-chunks

    Yield successive n-sized chunks from l.
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


class ImageGenerator:
    def __init__(self, cursor, db):
        self.cursor = cursor
        self.db = db

    def __iter__(self):
        return self

    def __next__(self):
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
                 db=os.path.join(user_data_dir(appname),
                                 'image_recognizer.db')):
        os.makedirs(user_data_dir(appname), exist_ok=True)
        self.db = db
        self.conn = {threading.current_thread(): sqlite3.connect(db, 15)}
        with self.conn[threading.current_thread()] as conn:
            conn.execute(queries.create_metadata_table)
            conn.execute(queries.create_paths_table)
            conn.executescript(queries.init_pragmas)

    def _init_connection(self):
        if not self.conn[threading.current_thread()]:
            self.conn[threading.current_thread()] = sqlite3.connect(self.db,
                                                                    15)

    def insert_images_data(self, images):
        self._init_connection()

        if type(images) is not list:
            images = [images]

        with self.conn[threading.current_thread()] as conn:
            signatures = [(image['hash'],
                           str(image['signature'])) for image in images]
            conn.executemany(queries.insert_image_metadata,
                             signatures)
            for image in images:
                paths = [(image['hash'], path) for path in
                         image['path']]
                conn.executemany(queries.insert_path,
                                 paths)
            logging.debug('Inserted {} in database'.format(signatures))

    def get_images_data(self, image_hashes):
        self._init_connection()

        if type(image_hashes) is not list:
            image_hashes = [image_hashes]

        with self.conn[threading.current_thread()] as conn:
            results = []
            for img_hash in image_hashes:
                res = conn.execute(queries.get_image_metadata,
                                   (img_hash,))
                data = res.fetchone()
                if data:
                    res = conn.execute(queries.get_paths,
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
        self._init_connection()

        with self.conn[threading.current_thread()] as conn:
            res = conn.execute(queries.get_all_images)
            if res:
                logging.info('Extracted all images from database')
                return ImageGenerator(res, self)

    def check_images_presence(self, images):
        self._init_connection()

        if type(images) is not list:
            images = [images]

        non_present_images = []
        with self.conn[threading.current_thread()] as conn:
            for img in images:
                res = conn.execute(queries.check_hash_existence,
                                   (img['hash'],))

                if not res.fetchone():
                    non_present_images.append(img)

            return non_present_images

    def add_imgs_paths(self, img):
        self._init_connection()

        if type(img) is not list:
            img = [img]

        path_list = []
        for simg in img:
            path_list += [(simg['hash'], path) for path in simg['path']]

        with self.conn[threading.current_thread()] as conn:
            conn.executemany(queries.insert_path,
                             path_list)

    def clean_orphan_paths(self):
        self._init_connection()

        with self.conn[threading.current_thread()] as conn:
            res = conn.execute(queries.get_all_paths)
            for path in res:
                if not os.path.isfile(path[1]):
                    conn.execute(queries.delete_path, (path[0],))

    def get_img_paths(self, img):
        self._init_connection()

        with self.conn[threading.current_thread()] as conn:
            results = conn.execute(queries.get_paths,
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


def calculate_batch_signatures(files_full_path):
    logging.debug('Starting batch signature processing')
    if type(files_full_path) is not list:
        files_full_path = [files_full_path]

    gis = ImageSignature()

    for path in files_full_path:
        calculate_file_signature(path, gis)


def calculate_signatures(root_path, db=DbMan()):
    db.clean_orphan_paths()

    with Pool(max_threads) as ppool:
        full_paths = []
        for path, subdirs, files in os.walk(root_path):
            for name in files:
                full_paths.append(PurePath(path, name))

        batch_size = round(len(full_paths)/max_threads)
        ppool.map(calculate_batch_signatures,
                  chunks(full_paths, batch_size))


def match_images(img1, img2, threshold, gis, db=DbMan()):
    dis = gis.normalized_distance(np.array(img1['signature']),
                                  np.array(img2['signature']))

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


def main():
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


if __name__ == "__main__":
    main()
