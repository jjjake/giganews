import os
import gzip
import dateutil.parser
import datetime
import time
from glob import glob

import chardet
import magic
import cStringIO


# clean_up()
# ________________________________________________________________________________________
def clean_up(group, identifier, date):
    """Delete all of a groups local mbox, index, and state files.

    :type group: str
    :param group: group name

    :type identifier: str
    :param identifier: the identifier for the given group.

    :rtype: bool
    :returns: True

    """
    #log.error('exception raised, cleaning up files.')
    glob_pat = '{g}.{d}.mbox*'.format(g=group, d=date)
    for f in glob(glob_pat):
        #log.error('removing {f}'.format(f=f))
        try:
            os.remove(f)
        except OSError:
            continue
    glob_pat = '{id}_state.json'.format(id=identifier)
    for f in glob(glob_pat):
        #log.error('removing {f}'.format(f=f))
        try:
            os.remove(f)
        except OSError:
            continue
    return True


# is_binary()
# ________________________________________________________________________________________
def is_binary(string):
    """Check if ``string`` contains binary data.

    :type string: str
    :param string: The text buffer to be tested for binary data.

    :rtype: bool
    :returns: True if `string` is binary, else False.

    """
    data_type = magic.from_buffer(string)
    return True if data_type == 'data' else False


# utf8_encode_str()
# ________________________________________________________________________________________
def utf8_encode_str(string, encoding='UTF-8'):
    """Attempt to detect the native encoding of `string`, and re-encode
    to utf-8

    :type string: str
    :param string: The string to be encoded.

    :rtype: str
    :returns: A utf-8 encoded string.

    """
    if not string:
        return ''
    src_enc = chardet.detect(string)['encoding']
    try:
        return string.decode(src_enc).encode(encoding)
    except:
        return string.decode('ascii', errors='replace').encode(encoding)


# inline_compress_chunk()
# ________________________________________________________________________________________
def inline_compress_chunk(chunk, level=1):
    """Compress a string using gzip.

    :type chunk: str
    :param chunk: The string to be compressed.

    :rtype: str
    :returns: `chunk` compressed.

    """
    b = cStringIO.StringIO()
    g = gzip.GzipFile(fileobj=b, mode='wb', compresslevel=level)
    g.write(chunk)
    g.close()
    cc = b.getvalue()
    b.close()
    return cc


# get_utc_iso_date()
# ________________________________________________________________________________________
def get_utc_iso_date(date_str):
    """Convert date str into a iso-formatted UTC date str, i.e.:
    yyyymmddhhmmss

    :type date_str: str
    :param date_str: date string to be parsed.

    :rtype: str
    :returns: iso-formatted UTC date str.

    """
    try:
        utc_tuple = dateutil.parser.parse(date_str).utctimetuple()
    except ValueError:
        try:
            date_str = ' '.join(date_str.split(' ')[:-1])
            utc_tuple = dateutil.parser.parse(date_str).utctimetuple()
        except ValueError:
            date_str = ''.join(date_str.split('(')[:-1]).strip(')')
            utc_tuple = dateutil.parser.parse(date_str).utctimetuple()
    date_object = datetime.datetime.fromtimestamp(time.mktime(utc_tuple))
    utc_date_str = ''.join([x for x in date_object.isoformat() if x not in '-T:'])
    return utc_date_str
