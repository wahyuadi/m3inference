#!/usr/bin/env python

import argparse
import pprint
import tweepy
import json
import jsonlines
import sys
import json
import logging
import MySQLdb
import shutil
import requests
from time import sleep
from os import walk, sep, makedirs, rename
from os.path import join, exists, basename, expanduser, dirname
from m3inference import M3Twitter
from datetime import datetime
from random import randint


M3_INPUT_DIR = ""

def get_db_conn():
    db = MySQLdb.connect(host="localhost",
            user="user",
            password="secret",
            db="dbname")

    return db

def data_to_db(db, sql_str):
    cur = db.cursor()
    cur.execute(sql_str)

def format_date(date_str):
    try:
        d = datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")
        return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(d.year, d.month, d.day, d.hour, d.minute, d.second)
    except Exception as e:
        pprint.pprint(e)
        return ""

#
#
# Fix image extension by renaming the file 
# e.g myimagejpeg becomes myimage.jpeg, or mypictpng becomes mypict.png
# This handles double extensions as well 
#
def fix_image_ext(file_path):
    file_name = basename(file_path)
    dir_name = dirname(file_path)
    valid_ext = ['jpeg', 'jpg', 'png', 'gif']

    slashpos = file_name.rfind(sep)
    if slashpos < 0:
        pass
    elif slashpos == 0:
        file_name = file_name[1:]
    elif slashpos == (len(file_name)-1):
        temp = file_name.split(sep)
        file_name = temp[:-1]
    else:
        temp = file_name.split(sep)
        file_name = temp[-1]

    has_extension = False

    #
    # 1. Find if filename ends with valid image extension
    # 2. If yes, put appropriate dot before that extension except if it already follows '.ext' pattern
    # 3. If no, infer from mime magic, then add extension accordingly  
    #
    dot_ext = ''
    for ext in valid_ext:
        if file_name.endswith(ext):
            has_extension = True
            dot_ext = '.' + ext
            dot_ext2 = dot_ext + dot_ext
            new_file = file_name

            # Handle double-dot cases first then double extension in one go
            # fastest method as shown in benchmark from https://stackoverflow.com/a/27086669
            new_file = new_file.replace('\\', '').replace('..', '.').replace(dot_ext2, dot_ext)
            break

    if not has_extension:
        dot_ext = '.' + get_magic_mime_extension(file_path)
        new_file = file_name + dot_ext
 
    new_path = join(dir_name, new_file)
    rename(file_path, new_path)
    print("renamed {} => {}".format(file_path, new_path))


#
# Fetch profile image so that M3Twitter.infer will be happy
#
def fetch_image(id, url, output_dir):

    # remove trailing directory separator
    if output_dir[-1] == sep:
        output_dir = output_dir[:-1]

    img_path = url
    img_path = img_path.replace("_normal", "_400x400")

    # be sure we have stripped query string part after ? in URL
    temp = img_path.split('?')[0]
    img_path = temp
    dotpos = img_path.rfind(".")
    
    # our dot position should indicate the image file extension. Check if such dot
    # is within top level domain (TLD)
    img_file_full = "{}/{}.{}".format(output_dir, id, img_path[dotpos+1:])
    img_file_resize = "{}/{}_224x224.{}".format(output_dir, id, img_path[dotpos+1:])

    # Skip if we already have such file:
    try:
        fh = open(img_file_resize, 'r') 
        print("Skipped: {}".format(img_file_resize))
        return img_file_resize

    except Exception as e:
        if img_path != "":
            print("Fetching {}...".format(img_path))
            response = requests.get(img_path, stream=True)
            with open(img_file_resize, 'wb') as out:
                shutil.copyfileobj(response.raw, out)
            del response

            # Make the file name standard; Minimize FileNotFound error on downstream processes
            fix_image_ext(img_file_resize)

            delay = (randint(1,37))/10
            print("will sleep for {} second.".format(delay))
            sleep(delay)
        else:
            print("fetch_image said: {}".format(e))

        return img_file_resize

       
    
# For parsing dict from inference result
def get_max_dict_val(ourdict):
    temp = 0
    result = ""
    for key in ourdict:
        if ourdict[key] > temp:
            temp = ourdict[key]
            result = key
    return result


def img_cache_file_exists(fname, cache_file_list):
    if fname == "":
        return False
    for cache in cache_file_list:
        if (fname in cache) or (fname == cache):
            return True


# Get local image file name for a given profile, if any
def get_local_image(id_str):
    result = ""
    for root, dirs, files in walk(M3_INPUT_DIR):
        for image_file in files:
            temp = image_file.split('_')
            if (id_str in image_file) or (temp[0] == id_str): 
                result = join(root, image_file)
    return result
            
#
# Given list of Twitter user profiles, generate the demography data
#
def populate_m3_profiles(logger, db, profiles, fname, cache_file_list):

    try:
        # users is a list of user final data before inserting to DB
        users = []
        user = {}

        for profile in profiles:
            id_str = profile['id_str']
            profile_img_path = profile['profile_image_url_https']
            pprint.pprint("id_str {}".format(id_str))

            # Download from Twitter if we haven't got the profile image file
            local_img_file = ""
            if not img_cache_file_exists(id_str, cache_file_list):
                local_img_file = fetch_image(id_str, profile_img_path, M3_INPUT_DIR)
            else:
                local_img_file = get_local_image(id_str)
            
            print("LOCAL IMAGE FILE {}".format(local_img_file))

            user['src_id'] = id_str
            user['name'] = profile['screen_name']
            user['bot_meter'] = profile['bot_score']
            user['media_type_id'] = '5'
            user['location_id'] = profile['location']
            user['user_created_at'] = format_date(profile['created_at'])
            user['date_inserted'] = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
            print("BEGIN PROFILE")
            pprint.pprint(profile)
            print("END PROFILE")

            m3_data = None
            try:

                fh = open(local_img_file, 'rb')
                print("EXCELLENT! Image file looks good: {}".format(local_img_file))
                m3twitter = M3Twitter()
                m3_data = m3twitter.transform_jsonl_object(profile)

            except Exception as e:
                # we give up on the profile image file. Use text model as a last resort
                print("Got PROBLEM with image file from {} (should be downloaded at {}). We use text model instead.".format(profile_img_path, local_img_file))
                m3twitter = M3Twitter(use_full_model=False)
                m3_data = m3twitter.transform_jsonl_object(profile)

            demography = m3twitter.infer([m3_data])

            user['age_group'] = get_max_dict_val(demography[m3_data['id']]['age'])
            user['gender'] = get_max_dict_val(demography[m3_data['id']]['gender'])
            user['is_organization'] = get_max_dict_val(demography[m3_data['id']]['org'])

            users.append(user)
            user = {}

        generate_result(users, fname, output_type='sql', to_db=True)                

    except Exception as e:
        generate_result(users, fname, output_type='json', to_db=False)                
        pprint.pprint(e)

#
# generate SQL / JSON files
#
def generate_result(users, output_file, output_type='sql', to_db=True):

    output_dir = "/home/adi/m3/output"

    if not exists(output_dir):
        makedirs(output_dir)
    
    if output_type == 'json':
        dot_ext = '.' + output_type
        output_path = join(output_dir, basename(output_file) + dot_ext)
        with open(output_path, 'w', encoding='utf-8') as out:
            json.dump(users, out)
        print("'{}' written.".format(output_path))

    sql_str = "INSERT INTO authors (src_id, name, media_type_id, location_id, age_group, gender, is_organization, bot_meter, user_created_at, date_inserted) VALUES "
    vals = []

    for u in users:
        s = "(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\")".format(u['src_id'], u['name'], u['media_type_id'], u['location_id'], u['age_group'], u['gender'], u['is_organization'], u['bot_meter'], u['user_created_at'], u['date_inserted'])
        vals.append(s)
        s = ""
    
    sql_vals = ", ".join(vals) + " ON DUPLICATE KEY UPDATE name=VALUES(name), age_group=VALUES(age_group), gender=VALUES(gender), is_organization=VALUES(is_organization), bot_meter=VALUES(bot_meter)"
    sql_str += sql_vals 

    if output_type == 'sql':
        dot_ext = '.' + output_type
        output_path = join(output_dir, basename(output_file) + dot_ext)
        with open(output_path, 'w', encoding='utf-8') as out:
            out.write(sql_str)
        print("'{}' written.".format(output_path))

    if to_db:
        db = get_db_conn()
        data_to_db(db, sql_str)
        
    

def init_logger():

    logger = logging.getLogger("fetch_profiles.log")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    return logger


def load_one_file(logger, file_name):
    lines = []
    line = {}
    input = expanduser(file_name)
    try:
        print("Opening {}...".format(input))
        with open(input, 'r') as f:
            for row in f:
                line = json.loads(row) 
                lines.append(line)

    except Exception as e:
        logger.warning("Error reading / appending {} content".format(input))
        pprint.pprint(e)

    return lines


#
# Sometimes the download interrupted for various reasons
# This function will get the start value to inform download_profile_images
# to lookup appropriate input files.
# It works as follows:
# 1. get image filename having latest modified time
# 2. extract its 'id'
# 3. get JSONL file containing that 'id'
# 4. get index of that JSONL within directory
#
def get_resume_file_pos(image_dir_name, input_dir_name):
    image_id = 0

    # inspired from https://stackoverflow.com/a/39327156
    latest_file = ""
    file_list = glob.glob(join(image_dir_name, "*"))
    if len(file_list) < 1: 
        return 0 
    else:
        latest_file = max(file_list, key=getctime)
    ######

    latest_file = basename(latest_file)
    temp = latest_file.split('_')
    if len(temp) > 0:
        image_id = temp[0]

    # Get the position of file containing image id in a list
    candidates_pos = []
    for root, dirs, files in walk(input_dir_name):
        for i, candidate in enumerate(files):
            if file_contains_str(candidate, image_id):
                candidates_pos.append(i)

    if len(candidates_pos) > 0:
        return candidates_pos[0]
    else:
        return 0

#
# Check if a file contains lookup string
#
def file_contains_str(file_name, lookup_str):
    try:
        with open(file_name, 'r') as fh:
            if lookup_str in fh.read():
                return True
            else:
                return False
    except Exception as e:
        return False

#

if __name__ == "__main__":

    logger = init_logger()

    parser = argparse.ArgumentParser(description='Process JSONL files containing Twitter user profiles and save demographic info to database.')
    parser.add_argument('--input-file', help='Input file containing JSONL files.')
    parser.add_argument('--input-dir', help='Input directory containing JSONL files.')
    args = parser.parse_args()

    if (args.input_dir == None) == (args.input_file == None):
        #sys.stderr.write("Please use either --input-dir OR --input-file")
        parser.print_help(sys.stderr)
        quit(1)

    db = get_db_conn()

    cache_files = []
    for root, dirs, files in walk(M3_INPUT_DIR):
        for cache_file in files:
            cache_files.append(join(root, cache_file))

    if (args.input_file):
        file_name = args.input_file
        lines = load_one_file(logger, file_name)

        # This line is core of the core
        populate_m3_profiles(logger, db, lines, file_name, cache_files)

    if (args.input_dir):
        dir_name = args.input_dir

        for root, dirs, files in walk(dir_name):
            for input_file in files:
                input_path = join(root, input_file)
                lines = load_one_file(logger, input_path)
                # This line is core of the core
                populate_m3_profiles(logger, db, lines, input_path, cache_files)

    db.close()
    sys.exit(0)


