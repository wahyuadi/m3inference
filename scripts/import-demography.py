#!/usr/bin/env python

import argparse
import pprint
import tweepy
import json
import jsonlines
import sys
import magic
import json
import logging
import redis
import MySQLdb
import shutil
import requests
import hashlib
import subprocess
from tqdm import tqdm
from collections import OrderedDict
from PIL import Image
from time import sleep
from os import walk, sep, makedirs, rename
from os.path import join, exists, basename, expanduser, dirname
from m3inference import M3Twitter
from datetime import datetime
from random import randint


M3_OUTPUT_DIR = ""
M3_CACHE_DIR = ""
JSONL_DIR = ""
MAX_BATCH_SIZE = 100
REDIS_USER_LIST_KEY = ""


auth = tweepy.OAuthHandler("", "")
auth.set_access_token("", "")
#

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=31, retry_count=111, retry_delay=29)
#api = tweepy.API(auth)


#
# Get DB connection
#
def get_db_conn():
    db = MySQLdb.connect(host="localhost",
            user="user",
            password="secret",
            db="dbname",
            charset="utf8mb4")

    return db


#
# Get total authors with bot_score which haven't been analyzed for demography
#
def get_total(db, *, last_hours=None, start_date=None, end_date=None):
    cur = db.cursor()

    try:

        if (last_hours is None) and (start_date is None) and (end_date is None):
            cur.execute("select count(b.id) as total from bot_score b join authors a on b.user_id = a.src_id")
            num_done = cur.fetchall()[0][0]

            cur.execute("select count(id) as total from bot_score")
            num_all = cur.fetchall()[0][0]

            num_wait = num_all - num_done

            return num_wait

        elif last_hours is not None:
            cur.execute("select count(id) from bot_score where date_checked >= date_sub(now(), interval %s hour)", [last_hours])
            return cur.fetchall()[0][0]

        elif (start_date is not None) and (end_date is not None):
            d1 = format_date2(start_date)
            d2 = format_date2(end_date)
            cur.execute("select count(id) from bot_score where date_checked >= %s and date_checked <= %s", [d1, d2])
            return cur.fetchall()[0][0]

        else:
            return 0

    except Exception as e:
        pprint.pprint("get_total got: {}".format(e))
        return 0


#
# Get a list of given data from DB query
#
def listify(data, index, get_all_column=False):
    if get_all_column:
        return [i for i in data]
    else:
        return [i[index] for i in data if index < len(i)]


#
# Insert to DB
#
def data_to_db(db, sql_str):
    try:
        cur = db.cursor()
        cur.execute(sql_str)
        cur.close()
        print("SUCCESS insert to DB (table: authors).")
    except Exception as e:
        pprint.pprint("data_to_db: Got {}".format(e))


#
# datetime string from Twitter usually in the form of "Wed May 27 11:22:33 +0000 2010"
# this will format to MySQL date time
# NOTE: Please BEWARE of strptime behavior, see datetime documentation!
#
def format_date(date_str):
    try:
        d = datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")
        return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(d.year, d.month, d.day, d.hour, d.minute, d.second)

    except Exception as e:
        pprint.pprint(e)
        return ""

#
# This will format input date from user in the form of YYYY-MM-DD
#
def format_date2(date_str):
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return "{:04d}-{:02d}-{:02d}".format(d.year, d.month, d.day)

    except Exception as e:
        pprint.pprint(e)
        return ""


#
# DOESN'T WORK: https://stackoverflow.com/a/38754958
# DIDN'T TRY THIS: https://stackoverflow.com/a/35859141
# TRIED THIS: https://stackoverflow.com/a/9459208
#
# Remove alpha channel from PNG file as to make Torch happy.
# Saves the output as JPG file instead, but the original profile file
# still refers to the PNG file (doesn't matter, anyway)
#
def remove_alpha(png_file, *, color=(255, 255, 255), save_as_new=True):

    out_file = png_file
    dotpos = out_file.rfind('.')

    ext = get_magic_mime_extension(png_file)

    if ext == 'png':
        try:

            if save_as_new:
                out_file = png_file[:dotpos] + '.jpg'

            png = Image.open(png_file)
            png.load() # required for png.split()
 
            background = Image.new("RGB", png.size, color)
            background.paste(png, mask=png.split()[3]) # 3 is the alpha channel

            background.save(out_file, 'JPEG', quality=100)
            print("remove_alpha: DONE saving '{}'.".format(out_file))

        except Exception as e:
            pprint.pprint("remove_alpha: {}".format(e))
            pass

    return out_file


#
# Get Twitter user's ID and its universal bot score from Bot-O-Meter
# from DB
# Note-to-self: arguments before '*' below are positional; 
# the others are keyword arguments
#
def get_twit_userid_from_db(db_conn, start_row, num_rows, *, project_id=None, start_date=None, end_date=None, last_hours=None, max_count=None):

    cur = db_conn.cursor()
    s = 0
    r = 100
    if start_row >= 0:
        s = start_row

    if num_rows > MAX_BATCH_SIZE:
        r = MAX_BATCH_SIZE

    if (start_date is not None) and (end_date is not None):
        #print("start_date end_date style")
        try:
            d1 = format_date2(start_date)
            d2 = format_date2(end_date)

            if project_id is not None:
                #print("with project id style")
                cur.execute("select m.author_id, b.scores_universal from mentions_%s m join bot_score b on m.author_id = b.user_id where m.date_created >= %s and m.date_created <= %s limit %s, %s", [project_id, d1, d2, s, r])

            else:
                #print("WITHOUT project id style")
                cur.execute("select user_id, scores_universal from bot_score where date_checked >= %s and date_checked <= %s limit %s, %s", [d1, d2, s, r])
            return cur.fetchall()

        except Exception as e:
            pprint.pprint("start_date end_date query error: {}".format(e))
            return []

    elif (last_hours is not None):
        #print("last_hours style")
        try:
            if project_id is not None:
                #print("with project id style")
                cur.execute("select m.author_id, b.scores_universal from mentions_%s m join bot_score b on m.author_id = b.user_id where m.date_created >= date_sub(now(), interval %s hour) limist %s, %s", [project_id, last_hours, s, r])
                
            else:
                #print("WITHOUT project id style")
                cur.execute("select user_id, scores_universal from bot_score where date_checked >= date_sub(now(), interval %s hour) limit %s, %s", [last_hours, s, r])
            return cur.fetchall()

        except Exception as e:
            pprint.pprint("last_hours query error: {}".format(e))
            return []

    else:
        #print("ANY OTHER style")
        try:
            if project_id is not None:
                #print("with project id style")
                cur.execute("select m.author_id, b.scores_universal from mentions_%s m join bot_score b on m.author_id = b.user_id limit %s, %s", [project_id, s, r])
            else:
                #print("WITHOUT project id style")
                cur.execute("select b.user_id, b.scores_universal from bot_score b where not exists (select 1 from authors a where b.user_id = a.src_id) limit %s, %s", [s, r])
            return cur.fetchall()

        except Exception as e:
            pprint.pprint("NON start_date end_date query error: {}".format(e))
            return []


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
# When the Twitter User ID is directly from DB, not JSONL files,
# we need to lookup there for the last ID.
#
def get_resume_db_pos(image_dir_name, db_conn):

    '''
    latest_userid = 0

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
        latest_userid = temp[0]

    try:
        cur = db_conn.cursor()
        cur.execute("select id from bot_score where user_id = %s limit 1", [latest_user_id]) 
        return cur.fetchall()[0][0]

    except:
        return 0
    '''
    try:
        cur = db_conn.cursor()
        cur.execute("select b.id from bot_score b where b.user_id = (select a.src_id from authors a order by a.id desc limit 1)")
        latest_user_id = cur.fetchall()[0][0]
        if latest_user_id:
            return latest_user_id
        else:
            return 0
    except Exception as e:
        log.warning("get_resume_db_pos got: {}".format(e))
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
# Fix image extension by renaming the file 
# e.g myimagejpeg becomes myimage.jpeg, or mypictpng becomes mypict.png
# This handles double extensions as well 
#
def fix_image_ext(file_path):

    valid_ext = ['jpeg', 'jpg', 'png', 'gif']

    input_path = ""

    if exists(file_path):
        #print("fix_image_ext: file_path '{}'.".format(file_path))
        input_path = file_path
    else:
        for ext in valid_ext:
            if exists(file_path + '.' + ext):
                input_path = file_path + '.' + ext 
                break
        #print("fix_image_ext: input_path by simple extension added: '{}' ".format(input_path))
    

    #
    # Check if we have PNG file version (e.g has alpha channel) of this file
    #
    new_file_path = file_path.replace('jpg', 'png').replace('jpeg', 'png')
    if exists(new_file_path):
        #print("fix_image_ext: Alternative PNG: {}.".format(new_file_path))
        input_path = new_file_path
    else:
        for ext in valid_ext:
            if exists(file_path + '.' + ext):
                input_path = file_path + '.' + ext 
                break
        #print("fix_image_ext: Alternative PNG: input_path by simple extension added: '{}'".format(input_path))

    # 
    # quick and dirty fix:
    #
    if "224x224.com" in file_path:
        leftdotpos = file_path.find('.')
        input_path = file_path[:leftdotpos]

        for ext in valid_ext:
            dot_ext = '.' + ext
            f = input_path + dot_ext
            if exists(f):
                rename(file_path, f)
                #return f
                input_path = f
                break

    if input_path == "":
        return "NOT FOUND: {} or {}".format(file_path, new_file_path)

    dir_name = ""
    file_name = input_path
    slashpos = file_name.rfind(sep)
    dotpos= file_name.rfind('.')

    if slashpos < 0:
        pass
    elif slashpos == 0:
        file_name = file_name[1:]
    elif slashpos == (len(file_name)-1):
        temp = file_name.split(sep)
        file_name = ''.join(temp[-2:-1])
        dir_name = sep.join(temp[:-2])
    elif slashpos > dotpos:
        temp = file_name[:dotpos].split(sep)
        file_name = temp[-1]
        dir_name = sep.join(temp[:-1])
    else:
        temp = file_name.split(sep)
        file_name = temp[-1]
        dir_name = sep.join(temp[:-1])

    has_extension = False

    #
    # 1. Find if filename ends with valid image extension
    # 2. If yes, put appropriate dot before that extension except if it already follows '.ext' pattern
    # 3. If no, infer from mime magic, then add extension accordingly  
    #
    dot_ext = ''
    for ext in valid_ext:

        dot_ext = '.' + ext
        dot_ext2 = dot_ext + dot_ext

        if dot_ext in file_name:
            if file_name.endswith(ext):
                has_extension = True

                # Handle double-dot cases first then double extension in one go
                # fastest method as shown in benchmark from https://stackoverflow.com/a/27086669
                new_file = file_name.replace('\\', '').replace('..', '.').replace(dot_ext2, dot_ext)
            else:
                has_extension = False
                file_name = file_name.replace('\\', '').replace('..', '.').replace(dot_ext, '')
            break



    if not has_extension:
        dot_ext = '.' + get_magic_mime_extension(input_path)
        new_file = file_name + dot_ext
 
    new_path = join(dir_name, new_file)
    #print("fix_image_ext: new_path {}".format(new_path))
    #rename(file_path, new_path)

    if exists(input_path):
        rename(input_path, new_path)

        if exists(new_path):
            return new_path
        else:
            return "FAILED to rename: {}".format(input_path)

 

#
# Get actual file extension from the file
#
def get_magic_mime_extension(file_path):
    try:
        mime = magic.from_file(file_path, mime=True)
        extension = ""
        splitted_mime = mime.split(sep)
        if len(splitted_mime) == 2:
            extension = splitted_mime[-1]
        return extension.lower()
    except Exception as e:
        pprint.pprint("get_magic_mime_extension: Got exception {}".format(e))
        return ""



#
# Fetch profile image so that M3Twitter.infer will be happy
#
def fetch_image(id_str, url, output_dir):

    # remove trailing directory separator
    if output_dir[-1] == sep:
        output_dir = output_dir[:-1]

    img_path = url
    img_path = img_path.replace("_normal", "_400x400")

    # be sure we have stripped query string part after ? in URL
    temp = img_path.split('?')[0]
    img_path = temp
    dotpos = img_path.rfind(".")
    slashpos = img_path.rfind(sep)

    if slashpos < dotpos: 
        img_file_resize = "{}/{}_224x224.{}".format(output_dir, id_str, img_path[dotpos+1:])
    else:
        img_file_resize = "{}/{}_224x224.{}".format(output_dir, id_str, 'jpg')

    print("fetch_image: '{}' => '{}' + '{}' ==> '{}' ===> '{}'.".format(url, output_dir, id_str, img_path, img_file_resize))
    # Skip if we already have such file:
    try:
        #print("Attempted to open: {}".format(img_file_resize))
        fh = open(img_file_resize, 'r') 

        # Remove alpha channel
        img_file_resize = remove_alpha(img_file_resize, save_as_new=True)

        #print("Skipped: {}".format(img_file_resize))

    except Exception as e:
        print("fetch_image: Got exception:")
        pprint.pprint(e)
        if img_path != "":
            #print("[WORKAROUND] Fetching {}...".format(img_path))
            response = requests.get(img_path, stream=True)

            with open(img_file_resize, 'wb') as out:
                shutil.copyfileobj(response.raw, out)
            del response
            #print("fetch_image: DONE saving '{}'.".format(img_file_resize))

            # Remove alpha channel
            img_file_resize = remove_alpha(img_file_resize, save_as_new=True)

            # Make the file name standard; Minimize FileNotFound error on downstream processes
            # UPDATE: No need to do this here. Has been handled by generate_demography_data
            #img_file_resize = fix_image_ext(img_file_resize)

            #delay = (randint(1,13))/10
            #print("will sleep for {} second.".format(delay))
            #sleep(delay)
        else:
            print("fetch_image said: {}".format(e))


    # we have saved as jpg inside remove_alpha; use that instead
    #return img_file_resize
    r = img_file_resize.replace('png', 'jpg') 
    if exists(r):
        return r
    else:
        return "NOT FOUND: {}".format(r)
       
    
# For parsing dict from inference result
def get_max_dict_val(ourdict):
    temp = -1 
    result = ""
    for key in ourdict:
        if ourdict[key] > temp:
            temp = ourdict[key]
            result = key
    return result


def img_cache_file_exists(fname, cache_file_list):
    if fname == "" or cache_file_list is None:
        return False
    for cache in cache_file_list:
        if (fname in cache) or (fname == cache):
            return True
    
    return False


# Get local image file name for a given profile, if any
def get_local_image(id_str):
    result = ""
    for root, dirs, files in walk(M3_CACHE_DIR):
        for image_file in files:
            temp = image_file.split('_')
            if (id_str in image_file) or (temp[0] == id_str): 
                result = join(root, image_file)
                break

    # We have replaced PNG with JPG version in remove_alpha; so we adjust here
    #return result
    r = result.replace('png', 'jpg') 
    if exists(r):
        return r
    else:
        return "NOT FOUND: {}".format(r)


#
# Given list of Twitter user profiles, generate the demography data
#
def generate_demography_data(logger, db, profiles, *, output_filename='output', cache_file_list=None, full_classifier=None, text_classifier=None):

    try:
        # users is a list of user final data before inserting to DB
        users = []

        #
        # WARNING: This ordered dict should have field list order that match those within generate_result function.
        #
        user = OrderedDict() 

        if full_classifier is None:
            m3twitter = M3Twitter()
        else:
            m3twitter = full_classifier

        if text_classifier is None:
            m3twitter_text = M3Twitter(use_full_model=False)
        else:
            m3twitter_text = text_classifier

        for profile in profiles:
            id_str = profile['id_str']
            profile_img_path = profile['profile_image_url_https']
            pprint.pprint("id_str {}".format(id_str))

            # Download from Twitter if we haven't got the profile image file
            local_img_file = ""
            if not img_cache_file_exists(id_str, cache_file_list):
                local_img_file = fix_image_ext(fetch_image(id_str, profile_img_path, M3_CACHE_DIR))
            else:
                local_img_file = get_local_image(id_str)
            
            m3_data = None

            try:

                with open(local_img_file, 'rb') as fh:
                    b = fh.read()
                    print("LOCAL IMAGE '{}' (size {} bytes)".format(local_img_file, len(b)))
                    pass

                print("EXCELLENT! Image file looks good: {}".format(local_img_file))
                m3_data = m3twitter.transform_jsonl_object(profile)

            except Exception as e:
                # we give up on the profile image file. Use text model as a last resort
                print("Got PROBLEM with image file from {} (should be downloaded at {}). We use text model instead.".format(profile_img_path, local_img_file))
                m3_data = m3twitter_text.transform_jsonl_object(profile)

            # sometimes profile_image_url_https does not end with file extension
            #print("BEFORE generate_demography_data: img_path from m3_data: {}".format(m3_data['img_path']))
            m3_data['img_path'] = fix_image_ext(m3_data['img_path'])
            #print("AFTER generate_demography_data: img_path from m3_data: {}".format(m3_data['img_path']))
            demography = m3twitter.infer([m3_data])

            #
            # THESE ORDER OF ASSIGNMENT SHOULD MATCH THE ORDER OF FIELD LIST AS DEFINED WITHIN generate_result FUNCTION
            #
            user['src_id'] = id_str
            user['name'] = profile['screen_name']
            user['media_type_id'] = '5'
            user['location_id'] = profile['location'].replace('""', '').replace("''", "").replace('"', '').replace("'", "") # some location may contain minute and second of coordinate
            user['age_group'] = get_max_dict_val(demography[m3_data['id']]['age'])
            user['gender'] = get_max_dict_val(demography[m3_data['id']]['gender'])
            user['is_organization'] = get_max_dict_val(demography[m3_data['id']]['org'])
            user['bot_meter'] = profile['bot_score']
            user['user_created_at'] = format_date(profile['created_at'])
            user['date_inserted'] = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")

            users.append(user)
            user = {}

        if output_filename is not None:
            generate_result(db, users, output_filename, output_type='sql', to_db=True)

    except Exception as e:
        if output_filename is not None:
            generate_result(db, users, output_filename, output_type='json', to_db=False)
            generate_result(db, users, output_filename, output_type='sql', to_db=False)
        pprint.pprint(e)

#
# generate SQL / JSON files
#
def generate_result(db_conn, users, output_file, output_type='sql', to_db=True):

    output_dir = "/datassd1/demography/m3/output"

    if not exists(output_dir):
        makedirs(output_dir)
    
    if output_type == 'json':
        dot_ext = '.' + output_type
        output_path = join(output_dir, basename(output_file) + dot_ext)
        with open(output_path, 'w', encoding='utf-8') as out:
            json.dump(users, out)
        print("'{}' written.".format(output_path))


    sql_str = ""

    if len(users) > 0:

        try:
            cur = db_conn.cursor()

            # multiple rows insert at once, using parameterized query
            values = ', '.join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"] * len(users))

            cur.execute("INSERT INTO authors (src_id, name, media_type_id, location_id, age_group, gender, is_organization, bot_meter, user_created_at, date_inserted) VALUES " + values + " ON DUPLICATE KEY UPDATE media_type_id=VALUES(media_type_id), location_id=VALUES(location_id), age_group=VALUES(age_group), gender=VALUES(gender), is_organization=VALUES(is_organization), bot_meter=VALUES(bot_meter), user_created_at=VALUES(user_created_at), date_inserted=VALUES(date_inserted);\r"
    , [user[key] for user in users for key in user])

            sql_str = cur._last_executed
            cur.close()
            print("SUCCESS insert to DB (table: authors).")

        except Exception as e:
            pprint.pprint("Got exception {}".format(e))


    if output_type == 'sql':
        dot_ext = '.' + output_type
        output_path = join(output_dir, basename(output_file) + dot_ext)
        with open(output_path, 'w', encoding='utf-8') as out:
            out.write(str(sql_str))
        print("'{}' written.".format(output_path))

        
    

def init_logger():

    logger = logging.getLogger("fetch_profiles.log")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    return logger


def get_redis_conn(server='localhost', port='6379'):
    r = redis.Redis(host=server, port=port)
    return r

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
# Download profile images from Twitter based on an profile input file (JSONL) file.
# Start means which index files should we start within inputfiles list, which is 
# useful for resuming download.
#
# How to resume download: 
# 1. check latest JPEG file downloaded
# 2. verify in which JSONL file this JPEG is contained
# 3. verify what index this JSONL file belong to, when we os.walk the input dir
# 4. start equals such index
#
def download_profile_images(logger, input_dir, start):
    output_dir = "/datassd1/demography/m3/cache"

    for root, dirs, inputfiles in walk(input_dir):
        for i, inputfile in enumerate(inputfiles):
            if i < start:
                continue
            logger.info("File {}".format(inputfile))
            with open(join(root, inputfile), 'r') as fh:
                for line in fh:
                    d = json.loads(line)
                    url = d['profile_image_url_https']
                    fetch_image(d['id_str'], url, output_dir)
                    #print("{} -> {} => {}".format(d['id'], d['profile_image_url'], d['profile_image_url_https']))

# Augment Twitter user data with bot score information from Db
def augment_user_data(user_data, data_pair):
    result = user_data
    if 'id_str' in result:
        for pair in data_pair:
            if result['id_str'] == pair[0]:
                result['bot_score'] = pair[1]
                break
    return result



# Augment Twitter user data with bot score information from Redis
def augment_user_data_redis(user_data, data_pair):
    result = user_data
    if 'id_str' in result:
        for pair in data_pair:
            p = pair.split(' ') 
            if result['id_str'] == p[0]:
                result['bot_score'] = p[1]
                break
    return result


# from https://stackoverflow.com/a/8419655
# calculate date difference between date1 and date2
# return 0 when fails to do so
def day_diff(date1, date2):
    try:
        d1 = datetime.strptime(date1, "%Y-%m-%d")
        d2 = datetime.strptime(date2, "%Y-%m-%d")
        return abs((d2 - d1).days)
    except Exception as e:
        return 0


#
def in_cache_add(redis_conn, id_str, add_unless_exists=True):
    author_key = 'tw-author-id-' + str(id_str)
    if redis_conn.get(author_key) is not None:
        return True
    else:
        if add_unless_exists:
            redis_conn.set(author_key, '')
            return True
        else:
            return False


# 
# Given Twitter User ID in Database, lookup user profiles
# from Twitter API and return a list of profile to pass to
# generate_demography_data directly, with optional writing to JSONL files.
#
# Note that start and rows corresponding to 'limit start, row' in query
# as executed inside get_twit_userid_from_db
# max_count = -1 ==> get all data stored in DB
#
def get_twitter_user_profiles(logger, db, redis_conn, start, rows, *, project_id=None, start_date=None, end_date=None, last_hours=None, max_count=300, write_to_file=False, full_classifier=None, text_classifier=None):

    if rows > MAX_BATCH_SIZE:
        rows = MAX_BATCH_SIZE

    m3twitter = None
    m3twitter_text = None

    if full_classifier is not None:
        m3twitter = full_classifier
    else:
        m3twitter = M3Twitter()

    if text_classifier is not None:
        m3twitter_text = text_classifier
    else:
        m3twitter_text = M3Twitter(use_full_model=False)

        
        
    lines = []
    total = 0
    if (start_date is not None) and (end_date is not None):
        total = get_total(db, last_hours=None, start_date=start_date, end_date=end_date)
    elif (last_hours is not None):
        total = get_total(db, last_hours=last_hours, start_date=None, end_date=None)
    else:
        # get total record from DB
        total = get_total(db, last_hours=None, start_date=None, end_date=None) 
    logger.warning("Got total data: {}, start {}, row {}".format(total, start, rows))

    # we loop over a batch of data since api.lookup_users can accommodate
    # maximum of 100 users per call
    for s in range(start, total, rows):

        # build list of Twitter User ID with parameters depending on various keyword arguments
        data_batch = []
        data_batch = get_twit_userid_from_db(db, s, rows, project_id=project_id, last_hours=last_hours, start_date=start_date, end_date=end_date, max_count=max_count)     

        if len(data_batch) < 1:
            logger.warning("get_twitter_user_profiles: empty batch at ({}, {}, {})".format(s, total, rows))
            continue
        else:
            logger.warning("get_twitter_user_profiles: we've got something (size {}) at ({}, {}, {})".format(len(data_batch), s, total, rows))
            batch_id_list = listify(data_batch, 0)

            if len(batch_id_list) > MAX_BATCH_SIZE:
                print("LEN batch_id_list {}".format(len(batch_id_list)))

            #batch_id_list = [i for i in batch_id_list if not in_cache_add(redis_conn, i, add_unless_exists=True)]

            try:
                # Lookup user profiles via Twitter API per batch
                if len(batch_id_list) > 0:
                    pprint.pprint("batch_id_list: {}".format(batch_id_list))
                    user_details = api.lookup_users(user_ids=batch_id_list)
                else:
                    print("EMPTY batch_id_list. Move on...")
                    continue
            except tweepy.error.TweepError as e:
                #tweepy.error.TweepError: [{'code': 17, 'message': 'No user matches for specified terms.'}]
                logger.warning(e)
                continue

            detail = {}
            # augment bot_score info to user details returned by Twitter API
            for user_detail in user_details:
                detail = user_detail._json
                detail = augment_user_data(detail, data_batch)
                lines.append(detail)
                detail = {}

            if write_to_file:
                fname = join(JSONL_DIR, "output" + str(s) + ".jsonl")
                with jsonlines.open(fname, mode='w') as writer:
                    writer.write_all(lines)
                logger.warning("{} written.".format(fname))

            generate_demography_data(logger, db, lines, output_filename=output_filename, cache_file_list=cache_files, full_classifier=m3twitter, text_classifier=m3twitter_text)
            lines = []





# 
# Given Twitter User ID in Redis, lookup user profiles
# from Twitter API and return a list of profile to pass to
# generate_demography_data directly, with optional writing to JSONL files.
#
#
def redis_get_twitter_user_profiles(logger, redis_conn, rows, *, max_count=300, write_to_file=False):

    def from_file(user_ids, data_batch):
        lines = []
        detail = {}

        print("We'll push our luck depending on existing JSONL Twitter user profiles...")
        for user_id in user_ids:
            print("grep {} -R {}".format(user_id, JSONL_DIR))
            output = subprocess.run(['grep', user_id, '-R', JSONL_DIR], capture_output=True, encoding="utf-8")
            grep_result = output.stdout.split('\n')
            for grep in grep_result:
                if (grep is None) or (grep == ''):
                    continue
                temp = grep.split(':')
                # temp[0] is the file name containing user ID we're looking for
                # the remainder temp[1:] is the json line
                detail = json.loads(':'.join(temp[1:]))
                detail = augment_user_data_redis(detail, data_batch)
                lines.append(detail)
                detail = {}
        print("GREP: Got {} result".format(len(lines)))
        return lines


    def from_api(user_details, data_batch):
        lines = []
        detail = {}
        # augment bot_score info to user details returned by Twitter API
        for user_detail in user_details:
            detail = user_detail._json
            detail = augment_user_data_redis(detail, data_batch)
            lines.append(detail)
            detail = {}
        return lines

    lines = []
    data_batch = []
    count = 0

    m3twitter = M3Twitter()
    m3twitter_text = M3Twitter(use_full_model=False)

    while (count <= max_count) and (redis_conn.llen(REDIS_USER_LIST_KEY) > 0):

        logger.warning("Building batch from Redis list (batch size {}, max {})...".format(rows, max_count))

        for i in range(0, rows):
            if redis_conn.llen(REDIS_USER_LIST_KEY) > 0:
                data_batch.append(redis_conn.lpop(REDIS_USER_LIST_KEY).decode('utf-8').replace("b'", "").replace("'", "").replace('"', ''))
                count += 1
            else:
                break
        

        if len(data_batch) < 1:
            logger.warning("get_twitter_user_profiles: empty batch.")
            continue
        
        else:
            print("ENTRI: {}".format(data_batch))
            batch_id_list = [i.split(' ')[0] for i in data_batch]
            #print('\n'.join(batch_id_list))
            logger.warning("{} built.".format(len(data_batch)))

            try:
                # Lookup user profiles via Twitter API per batch
                if len(batch_id_list) > 0:
                    user_details = api.lookup_users(user_ids=batch_id_list)
                else:
                    print("EMPTY batch_id_list. Move on...")
                    continue

                lines = from_api(user_details, data_batch)
                write_to_file = True

            except tweepy.error.TweepError as e:
                # often we receive this error on suspended / deleted accounts
                #tweepy.error.TweepError: [{'code': 17, 'message': 'No user matches for specified terms.'}]
                logger.warning(e)
                # when we're lucky, there may already be a JSONL file containing user profiles, so use it instead.
                lines = from_file(batch_id_list, data_batch)
                # we already got result from JSONL files; don't write another JSONL files
                write_to_file = False 

            u = hashlib.sha256(bytes(json.dumps(lines), encoding='utf-8')).hexdigest()
            output_filename = "output_" + str(u) 

            if write_to_file:
                fname = join(JSONL_DIR, output_filename + ".jsonl")
                with jsonlines.open(fname, mode='w') as writer:
                    writer.write_all(lines)
                logger.warning("{} written.".format(fname))

            generate_demography_data(logger, db, lines, output_filename=output_filename, cache_file_list=cache_files, full_classifier=m3twitter, text_classifier=m3twitter_text)
            lines = []

        data_batch = []


#
# Build bot user list from DB into Redis list
#
def build_user_list(logger, db_conn, redis_conn):

    # so we don't accidentally add to Redis
    #return -1

    cur = db_conn.cursor()
    try:
        #logger.warning("Building user list. Please wait...")
        print("Building user list. Please wait...")
        cur.execute("select user_id, scores_universal from bot_score order by id")
        #cur.execute("select b.user_id, b.scores_universal from bot_score b left join authors a on b.user_id = a.src_id where a.src_id is null")
        bot_data = cur.fetchall()
        #logger.info("bot_score queried. Got {} data.".format(len(bot_data)))
        print("bot_score queried. Got {} data.".format(len(bot_data)))

        cur.execute("select src_id from authors order by id")
        author_data = cur.fetchall()
        #logger.info("author queried. Got {} data.".format(len(author_data)))
        print("author queried. Got {} data.".format(len(author_data)))

        #logger.info("listifying authors...")
        print("listifying authors...")
        author_data = listify(author_data, 0)
        #logger.info("author data listified.")
        print("author data listified.")

        count = 0

        print("creating bot_id set...")
        bot_id_set = set([b[0] for b in bot_data])
        print("DONE")

        #logger.info("creating author set...")
        print("creating author set...")
        author_id_set = set(author_data)
        #logger.info("DONE.")
        print("DONE.")

        print("bot_id_set: {}, author_id_set: {}".format(len(bot_id_set), len(author_id_set)))
        
        #logger.warning("Pushing to Redis...")
        print("Pushing to Redis...")
        #unprocessed = tqdm([str(b[0] + " " + b[1]) if b[0] not in author_set for b in bot_data])
        new_id_set = bot_id_set - author_id_set
        unprocessed = [" ".join(i) for i in bot_data if i[0] in new_id_set]

        pbar = tqdm(unprocessed)
        for item in pbar:
            #redis_conn.rpush(REDIS_USER_LIST_KEY, ' '.join([b.encode('utf-8') for b in bot]))
            redis_conn.lpush(REDIS_USER_LIST_KEY, item)
            count += 1
            pbar.update(1)
            pbar.set_description("Pushed '{}'.".format(item))
        pbar.close()

        #ratio = count / len(bot_data)
        #print("DONE pushing {} data to Redis (ratio {.3f}%, bot_data {}, author_data {}).".format(count, ratio * 100, len(bot_data), len(author_data)))
        return count
    
    except Exception as e:
        pprint.pprint("build_user_list: {}".format(e))
        return 0

#
# QUICK FIX: Build misaligned authors list from DB into Redis list
#
def build_user_list2(logger, db_conn, redis_conn):

    cur = db_conn.cursor()
    try:
        logger.warning("Building user list for 'misaligned data'. Please wait...")
        # Due to bug in previous version of script caused by lack of use of ordered dict, some inserted row has mismatched columns and values
        cur.execute("select a.src_id, b.scores_universal from authors a join bot_score b on a.src_id = b.user_id where a.media_type_id <> '5' or a.location_id = '5' or a.date_inserted = '0000-00-00 00:00:00' or a.user_created_at = '0000-00-00 00:00:00' or a.gender not like '%male' or a.is_organization not like '%org' or a.age_group not in ('<=18', '19-29', '30-39', '>=40') order by rand()")
        bot_data = cur.fetchall()

        logger.warning("Got {} misaligned bot user list.".format(len(bot_data)))

        logger.warning("Pushing to Redis...")
        count = 0
        for i, bot in enumerate(bot_data):
            redis_conn.lpush(REDIS_USER_LIST_KEY, ' '.join([str(b.encode('utf-8')) for b in bot]))
            print("{}. Pushed '{}'".format(i, bot))
            count = i

        logger.warning("DONE")
        return count
    
    except Exception as e:
        pprint.pprint("build_user_list: Got {}".format(e))
        return 0






if __name__ == "__main__":

    logger = init_logger()

    parser = argparse.ArgumentParser(prog='mk-import-demography.py', description='Process Twitter user profiles data (from DB / JSONL files) and save demographic info to database.', add_help=True)

    parser.add_argument('project_id', type=int, nargs='?', help='Project ID of the mentions we want to get Twitter users list of')
    parser.add_argument('--max-count', type=int, default=10000, help="Maximum total data should be processed (DEFAULT: 10000)")
    parser.add_argument('--build-list', action='store_true', help="Build a cache list of User ID to insert into Redis list")
    parser.add_argument('--consume-list', action='store_true', help="Consume user ID list from Redis and generate and classify their data")
    parser.add_argument('--cache-server', default='localhost', help="Consume user ID list from Redis and generate and classify their data")

    group_db_row = parser.add_argument_group('group_db_row')
    group_db_row.add_argument('--start-row', type=int, default=0, help='DB record number to start from (DEFAULT: 0)')
    group_db_row.add_argument('--batch-size', type=int, default=50, help='number of user profiles to get at once (MAX: 100, DEFAULT:50)')

    group_db_period = parser.add_argument_group('group_db_period')
    group_db_period.add_argument('--start-date', help='DB date start period')
    group_db_period.add_argument('--end-date', help='DB date end period')

    group_db_lastn = parser.add_argument_group('group_db_lastn')
    group_db_lastn.add_argument('--last-hours', type=int, help='DB last N hours')

    group_file_dir = parser.add_mutually_exclusive_group(required=False)
    group_file_dir.add_argument('--input-file', help='full path to input file')
    group_file_dir.add_argument('--input-dir', help='full path to input directory containing one or more input files')

    args = parser.parse_args()

    db = get_db_conn()
    redis_conn = get_redis_conn(server='localhost', port='6379')

    #  populate list of downloaded ('cached') image files
    cache_files = []
    for root, dirs, files in walk(M3_CACHE_DIR):
        for cache_file in files:
            cache_files.append(join(root, cache_file))


    start = 0
    rows = 10

    m3twitter = M3Twitter()
    m3twitter_text = M3Twitter(use_full_model=False)

    # Execute user's request as specified in their command line arguments
    if (args.input_file):
        file_name = args.input_file
        lines = load_one_file(logger, file_name)
        generate_demography_data(logger, db, lines, output_filename=file_name, cache_file_list=cache_files, full_classifier=m3twitter, text_classifier=m3twitter_text)

    elif (args.input_dir):
        dir_name = args.input_dir
        for root, dirs, files in walk(dir_name):
            for input_file in files:
                input_path = join(root, input_file)
                lines = load_one_file(logger, input_path)
                # generate demography data
                generate_demography_data(logger, db, lines, output_filename=input_path, cache_file_list=cache_files, full_classifier=m3twitter, text_classifier=m3twitter_text)


    elif (args.build_list):
        if (args.cache_server):
            redis_conn = get_redis_conn(server=args.cache_server, port='6379')

        count = build_user_list(logger, db, redis_conn)
        logger.warning("build_user_list: Got {} data".format(count))
        #count = build_user_list2(logger, db, redis_conn)
        #logger.warning("build_user_list2: Got {} data".format(count))


    elif (args.consume_list):
        data_size = 10
        max_count = 100000
        if (args.cache_server):
            try:
                redis_conn = get_redis_conn(server=args.cache_server, port='6379')
            except Exception as e:
                pprint.pprint(e)
                sys.exit(1)

        if (args.batch_size and args.batch_size <= MAX_BATCH_SIZE):
            data_size = args.batch_size

        if (args.max_count):
            max_count = args.max_count

        redis_get_twitter_user_profiles(logger, redis_conn, data_size, max_count=max_count, write_to_file=True)
        logger.warning("DONE consuming list (max_count {}, batch_size {})".format(args.max_count, data_size))

    elif (args.last_hours is not None):
        last_hours = args.last_hours
        start = 0
        rows = 10 
        output_filename = join(M3_OUTPUT_DIR, "output") # if not defined, it will directly execute insert to DB.

        get_twitter_user_profiles(logger, db, redis_conn, start, rows, max_count=None, project_id=None, start_date=None, end_date=None, last_hours=last_hours, write_to_file=False, full_classifier=m3twitter, text_classifier=m3twitter_text)
    
    elif (args.start_date) and (args.end_date):
        start_date = args.start_date
        end_date = args.end_date
        start = 0
        rows = 100
        output_filename = join(M3_OUTPUT_DIR, "output") # if not defined, it will directly execute insert to DB.

        get_twitter_user_profiles(logger, db, redis_conn, start, rows, max_count=None, project_id=None, start_date=start_date, end_date=end_date, last_hours=None, write_to_file=False, full_classifier=m3twitter, text_classifier=m3twitter_text)

    elif (args.project_id) or (args.start_row) or (args.batch_size) or (args.max_count):

        project_id = args.project_id
        start_date = args.start_date
        end_date = args.end_date
        start = args.start_row
        #start = get_resume_db_pos(M3_CACHE_DIR, db)
        rows = args.batch_size
        max_count = args.max_count
        output_filename = join(M3_OUTPUT_DIR, "output") # if not defined, it will directly execute insert to DB.

        print("HEY HO")

        get_twitter_user_profiles(logger, db, redis_conn, start, rows, max_count=max_count, project_id=project_id, start_date=start_date, end_date=end_date, last_hours=last_hours, write_to_file=False, full_classifier=m3twitter, text_classifier=m3twitter_text)

    else:
        parser.print_help()

    db.close()
    sys.exit(0)


