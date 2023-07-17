import io
from collections import defaultdict
from loguru import logger
from flask import session

stringio_dict = defaultdict(lambda: io.StringIO())

def get_io_buffer():
    session_id = session['session_id']
    return stringio_dict[session_id]