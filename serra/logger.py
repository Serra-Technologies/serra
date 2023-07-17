import io
from collections import defaultdict
from loguru import logger
from flask import session

stringio_dict = defaultdict(lambda: io.StringIO())

def get_io_buffer():
    session_id = session['session_id']
    return stringio_dict[session_id]

def log(message):
    # get the logger corresponding to the info
    get_io_buffer().write(message)

def read_and_flush_logs():
    logs = get_io_buffer().getvalue()
    get_io_buffer().truncate(0)
    return logs
