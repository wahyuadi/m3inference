#!/usr/bin/env python

import os
import sys
import magic
import pprint
import re


JSONL_DIR=""
IMAGE_DIR=""

def get_magic_mime_extension(file_path):
    mime = magic.from_file(file_path, mime=True)
    extension = ""
    splitted_mime = mime.split(os.sep)
    if len(splitted_mime) == 2:
        extension = splitted_mime[-1]
    return extension

#
# Fix image extension by renaming the file 
# e.g myimagejpeg becomes myimage.jpeg, or mypictpng becomes mypict.png
#
def fix_image_ext(file_path):
    file_name = os.path.basename(file_path)
    dir_name = os.path.dirname(file_path)
    valid_ext = ['jpeg', 'jpg', 'png', 'gif']

    slashpos = file_name.rfind(os.sep)
    if slashpos < 0:
        pass
    elif slashpos == 0:
        file_name = file_name[1:]
    elif slashpos == (len(file_name)-1):
        temp = file_name.split(os.sep)
        file_name = temp[:-1]
    else:
        temp = file_name.split(os.sep)
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
            # Handle doubledot cases first then double extension in one go
            # https://stackoverflow.com/a/27086669
            new_file = new_file.replace('\\', '').replace('..', '.').replace(dot_ext2, dot_ext)
            break

    if not has_extension:
        dot_ext = '.' + get_magic_mime_extension(file_path)
        new_file = file_name + dot_ext
 
    new_path = os.path.join(dir_name, new_file)
    os.rename(file_path, new_path)
    print("renamed {} => {}".format(file_path, new_path))

               

if  __name__ == "__main__":

    '''
    input_dir = ""
    if len(sys.argv) != 2:
        print("Usage: {} <input_dir> ".format(sys.argv[0]))
        print("Directory containing image to be fixed")
        sys.exit(1)
    else:
        input_dir = sys.argv[1]
    '''


    try:
        for root, dirs, files in os.walk(IMAGE_DIR):
            for img_file in files:
                print("inside loop: {}".format(os.path.join(IMAGE_DIR, img_file)))
                fix_image_ext(os.path.join(IMAGE_DIR, img_file))

    except Exception as err:
        pprint.pprint(err)
        sys.exit(1)
