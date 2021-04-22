#!/usr/bin/env python3
# @Zijian Wang

import argparse
import glob
import json
import logging
import os
import urllib.request
import requests
import shutil
import hashlib
#import PythonMagick as Magick
from io import BytesIO
from time import sleep
from random import randint

from PIL import Image
from tqdm import tqdm

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger(__name__)


# Wahyu: plan B when urllib failed to download Twitter image file
def fetch_image(url, img_out_path, img_out_path_fullsize=None):
    delay = randint(2,37)/10
    print("Sleeping for {} second...".format(delay))
    sleep(delay)
    print("Woke up. Retry downloading '{}'...".format(url))
    try:
        response = requests.get(url, stream=True)
        img_data = response.content
        if img_out_path_fullsize != None:
            with open(img_out_path_fullsize, "wb") as fh:
                fh.write(img_data)
    except Exception as err:
        logger.warn("fetch_image (inside preprocess.py) got exception: {}".format(err))
    return img_data


def download_resize_img(url, img_out_path, img_out_path_fullsize=None):
    # url=url.replace("_200x200","_400x400")
    try:
        img_data = urllib.request.urlopen(url)
        img_data = img_data.read()
        if img_out_path_fullsize != None:
            with open(img_out_path_fullsize, "wb") as fh:
                fh.write(img_data)
    except urllib.error.HTTPError as err:
<<<<<<< HEAD
        logger.warn("Error fetching profile image from '{}'. HTTP error code was {}. Attempting to fetch with requests...".format(url, err.code))
        img_data = fetch_image(url, img_out_path, img_out_path_fullsize)
        #raise err
=======
        logger.warn("Error fetching profile image from Twitter. HTTP error code was {}.".format(err.code))
        return None
>>>>>>> 35c3a9e9844aa6b45ab831f5c9d7fdea036369fc

    return resize_img(BytesIO(img_data), img_out_path, force=True,url=url)


<<<<<<< HEAD
# Wahyu: modified by adding failover when Pillow failed to resize
# I choose PythonMagick as plan B.
def resize_img(img_path, img_out_path, filter=Image.BILINEAR, force=False):
=======
def resize_img(img_path, img_out_path, filter=Image.BILINEAR, force=False,url=None):
>>>>>>> 35c3a9e9844aa6b45ab831f5c9d7fdea036369fc
    try:

        '''
        # Backup and hash the filename
        ori_name = os.path.basename(img_path)
        ori_dir = os.path.dir(img_path)
        copied_dir = "/tmp"
        copied_name = ori_name 

        temp_name = hashlib.md5(copied_name.encode('utf-8')).hexdigest()
        temp_path = os.path.join(temp_dir, temp_name)
        shutil.copy(img_path, temp_path)
        ###################
        '''

        img = Image.open(img_path).convert("RGB")
        if img.size[0] + img.size[1] < 400 and not force:
            logger.info(f'{img_path} / {url} is too small. Skip.')
            return
        img = img.resize((224, 224), filter)
        img.save(img_out_path)
    except Exception as e:
<<<<<<< HEAD
        #logger.warning(f'Error when resizing {img_path}\nThe error message is {e}\n')
        # Failover using PythonMagick
        logger.info('Failed resizing with Pillow, will try PythonMagick shortly...')
        #img_path = os.path.join(ori_dir, ori_name)
        #shutil.copy(temp_path, img_path)
        #img = Magick.Image(img_path)
        #img.resize("224x224")
        #img.write(img_out_path)
        ###################

=======
        logger.warning(f'Error when resizing {img_path} / {url}\nThe error message is {e}\n')
>>>>>>> 35c3a9e9844aa6b45ab831f5c9d7fdea036369fc


def resize_imgs(src_root, dest_root, src_list=None, filter=Image.BILINEAR, force=False):
    if not os.path.exists(src_root):
        raise FileNotFoundError(f"{src_root} does not exist.")

    if not os.path.exists(dest_root):
        os.makedirs(dest_root)

    src_list = glob.glob(os.path.join(src_root, '*')) if src_list is None else src_list

    des_set = set([os.path.relpath(img_path, dest_root).replace('.jpeg', '')
                   for img_path in glob.glob(os.path.join(dest_root, '*'))])

    for img_path in tqdm(src_list, desc='resizing images'):

        img_name = os.path.splitext(os.path.relpath(img_path, src_root))[0]
        if not force and img_name in des_set:
            logger.debug(f"{img_name} exists. Skipping...")
            continue
        else:
            out_path = os.path.join(dest_root, img_name) + '.jpeg'
            logger.debug(f'{img_name} not found in {dest_root}. Resizing to {out_path}')
            resize_img(img_path, out_path, filter=filter, force=force)


def update_json(jsonl_filepath, jsonl_outfilepath, src_root, dest_root):
    logger.info(f'Loading jsons from {jsonl_filepath}')
    list_of_jsons = []
    with open(jsonl_filepath) as infile:
        for line in infile:
            list_of_jsons.append(json.loads(line))

    new_jsons = []
    for j in list_of_jsons:
        img_path = j['img_path']
        j['img_path'] = os.path.splitext(os.path.abspath(os.path.join(dest_root, os.path.relpath(img_path, src_root))))[
                            0] + '.jpeg'
        new_jsons.append(j)
    logger.info(f'Saving jsons to {jsonl_outfilepath}')
    with open(jsonl_outfilepath, 'w') as outfile:
        for j in list_of_jsons:
            outfile.write(json.dumps(j, sort_keys=True) + '\n')

