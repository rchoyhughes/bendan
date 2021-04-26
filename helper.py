import hashlib
import base64
from datetime import timezone
import datetime
import math
from PIL import Image


def hash_string(string: str) -> str:
    hashstr = hashlib.sha3_224(string.encode()).digest()
    b64 = base64.urlsafe_b64encode(hashstr).decode()
    return b64[:len(b64) - 2]


def get_timestamp() -> int:
    dt = datetime.datetime.now(timezone.utc)
    utc_time = dt.replace(tzinfo=timezone.utc)
    utc_timestamp = utc_time.timestamp()
    return math.floor(utc_timestamp)


def time_string(post_timestamp: int) -> str:
    now = get_timestamp()
    diff = now - post_timestamp
    if diff < 60:
        return 'Posted just now'
    elif 60 < diff < (60 * 60):
        if math.floor(diff/60) == 1:
            return 'Posted 1 minute ago'
        return 'Posted ' + str(math.floor(diff / 60)) + ' minutes ago'
    else:
        diff_mins = round(diff / 60)
        diff_hours = math.floor(diff_mins / 60)
        if diff_hours >= 24:
            if math.floor(diff_hours / 24) == 1:
                return 'Posted 1 day ago'
            return 'Posted ' + str(math.floor(diff_hours / 24)) + ' days ago'
        if diff_mins % 60 > 30:
            if diff_hours == 1:
                return 'Posted 1 hour and 30 minutes ago'
            return 'Posted ' + str(diff_hours) + ' hours and 30 minutes ago'
        else:
            if diff_hours == 1:
                return 'Posted 1 hour ago'
            return 'Posted ' + str(diff_hours) + ' hours ago'


def make_img_square(filename):
    im = Image.open(filename)
    im = crop_max_square(im)
    im.save(filename)


def crop_center(pil_img, crop_width, crop_height):
    img_width, img_height = pil_img.size
    return pil_img.crop(((img_width - crop_width) // 2,
                         (img_height - crop_height) // 2,
                         (img_width + crop_width) // 2,
                         (img_height + crop_height) // 2))


def crop_max_square(pil_img):
    return crop_center(pil_img, min(pil_img.size), min(pil_img.size))


if __name__ == '__main__':
    s = get_timestamp()
    print(s)
