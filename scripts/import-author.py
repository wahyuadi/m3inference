import pprint
import tweepy
import json
import jsonlines
import logging
import sys
import time
import glob
import requests
import shutil
import magic
import MySQLdb
from time import sleep
from random import randint
from os import sep, walk, rename, stat
from os.path import getctime, join, dirname, basename, exists
from m3inference import M3Twitter

JSONL_DIR=""
IMAGE_DIR = ""
# this is set by Twitter as documented somewhere in Tweepy's api.lookup_users documentation
MAX_BATCH_SIZE = 100 

auth = tweepy.OAuthHandler("", "")
auth.set_access_token("", "")

#api = tweepy.API(auth)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# Get DB Connection
def get_db_conn():
    db = MySQLdb.connect(host="localhost",
            user="user",
            password="secret",
            db="dbname")
    return db

# Get total record from DB
def get_total(db):
    cur = db.cursor()
    cur.execute("select count(id) as total from bot_score")
    return cur.fetchall()[0][0]

# Get a list of given data from DB query
def listify(data, index, get_all_column=False):
    if get_all_column:
        return [i for i in data]
    else:
        return [i[index] for i in data if index < len(i)]

#
# Get Twitter user's ID and its universal bot score from Bot-O-Meter
# from database
#
def get_twit_userid_from_db(db, start, rows):
    cur = db.cursor()
    cur.execute("select user_id, scores_universal from bot_score limit {}, {}".format(start, rows))
    return cur.fetchall()

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
# Get actual file extension from the file
#
def get_magic_mime_extension(file_path):
    if exists(file_path): 
        mime = magic.from_file(file_path, mime=True)
        extension = ""
        splitted_mime = mime.split(sep)
        if len(splitted_mime) == 2:
            extension = splitted_mime[-1]
        return extension
    else:
        return "unknown"


#
# Fix image extension by renaming the file 
# e.g myimagejpeg becomes myimage.jpeg, or mypictpng becomes mypict.png
# This handles double extensions as well 
#
def fix_image_ext(file_path):

    valid_ext = ['jpeg', 'jpg', 'png', 'gif']

    input_path = ""

    if exists(file_path):
        print("fix_image_ext: file_path '{}'.".format(file_path))
        input_path = file_path
        return input_path
    else:
        for ext in valid_ext:
            if exists(file_path + '.' + ext):
                input_path = file_path + '.' + ext 
                print("fix_image_ext: input_path by simple extension added: '{}' ".format(input_path))
                return input_path
    

    #
    # Check if we have PNG file version (e.g has alpha channel) of this file
    #
    new_file_path = file_path.replace('jpg', 'png').replace('jpeg', 'png')
    if exists(new_file_path):
        print("fix_image_ext: Alternative PNG: {}.".format(new_file_path))
        input_path = new_file_path
        return input_path

    else:
        for ext in valid_ext:
            if exists(new_file_path + '.' + ext):
                input_path = new_file_path + '.' + ext 
                print("fix_image_ext: Alternative PNG: input_path by simple extension added: '{}'".format(input_path))
                return input_path

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
                logger.warning("f {}".format(f))
                #rename(file_path, f)
                logger.warning("from quick fix: f {}, input_path {}".format(f, input_path))
                input_path = f
                return input_path

    if input_path == "":
        logger.warning("NOT FOUND after multiple attempt: {} or {}".format(file_path, new_file_path))
        return file_path

    dir_name = ""
    file_name = input_path
    slashpos = file_name.rfind(sep)
    dotpos = file_name.rfind('.')

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
    print("fix_image_ext: new_path {}".format(new_path))
    #rename(file_path, new_path)

    if exists(input_path):
        rename(input_path, new_path)

        if exists(new_path):
            return new_path
        else:
            logger.warning("FAILED to rename: {} to {}".format(input_path, new_path))
            return input_path
    else:
        return new_path

 

#
# Fix image extension by renaming the file 
# e.g myimagejpeg becomes myimage.jpeg, or mypictpng becomes mypict.png
# This handles double extensions as well 
#
def fix_image_ext_broken(file_path):
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
    new_file = ''
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

    return new_path

               
#
# Fetch the image file and put in specified directory
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
        print("Skipped existing: {}".format(img_file_resize))
        return None

    except Exception as e:
        img_file_resize = fix_image_ext(img_file_resize)
        if img_path != "" and (img_file_resize != "" or img_file_resize is not None):
            print("Fetching {}, will save to {}...".format(img_path, img_file_resize))
            response = requests.get(img_path, stream=True)
            with open(img_file_resize, 'wb') as out:
                shutil.copyfileobj(response.raw, out)
            del response

            # Make the file name standard; Minimize FileNotFound error on downstream processes
            return img_file_resize

            delay = (randint(1,17))/10
            print("will sleep for {} second.".format(delay))
            sleep(delay)
        else:
            print("fetch_image said: {}".format(e))

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
    output_dir = "/home/adi/m3/cache"

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


# Augment Twitter user data with bot score information, if any
def augment_user_data(user_data, data_pair):
    result = user_data
    if 'id_str' in result:
        for pair in data_pair:
            if result['id_str'] == pair[0]:
                result['bot_score'] = pair[1]
                break
    return result
# 
# Given a list of Twitter User ID in Database, lookup user profiles
# from Twitter API and populate files containing Twitter user profiles per batch 
# of size 'row'
# Note that start and rows corresponding to 'limit start, row' in query
# as executed inside get_twit_userid_from_db
# max_count = -1 ==> get all data stored in DB
#
def get_twitter_profiles(logger, db, start, rows, max_count=300):
    lines = []
    total = get_total(db) # get total record from DB
    logger.warning("Got total data: {}, start {}, row {}".format(total, start, rows))

    #
    # Boundaries checking
    #
    end = 0
    if (max_count < 0) or (max_count > total):
        end = total
    else:
        end = max_count

    if start < 0:
        start = 0
    if rows > MAX_BATCH_SIZE:
        rows = MAX_BATCH_SIZE

    # we loop over a batch of data since api.lookup_users can accommodate
    # maximum of 100 users per call
    for s in range(start, end, rows):

        # build list of Twitter User ID
        data_batch = get_twit_userid_from_db(db, s, rows)
        if len(data_batch) < 1:
            break
        else:
            batch_id_list = listify(data_batch, 0)

        try:
            # Lookup user profiles via Twitter API per batch
            user_details = api.lookup_users(user_ids=batch_id_list)
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

        fname = join(JSONL_DIR, "output" + str(s) + ".jsonl")
        with jsonlines.open(fname, mode='w') as writer:
            writer.write_all(lines)
        logger.warning("{} written.".format(fname))
        lines = []


if __name__ == "__main__":

    logger = logging.getLogger("fetch_profiles.log")
    logger.setLevel(logging.WARN)
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARN)
    formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    db = get_db_conn()
    #logger.warning("Connected to DB.")

    start = get_resume_file_pos(IMAGE_DIR, JSONL_DIR) 
    #start = 5000
    rows = 100
    max_count = 1000000
    logger.warning("START POS: {}".format(start))

    # request Twitter for list of user profiles data
    #get_twitter_profiles(logger, db, start, rows, max_count)
    download_profile_images(logger, JSONL_DIR, start)

    logger.warning("Finished getting Twitter user profiles.")
    sys.exit(0)

    
