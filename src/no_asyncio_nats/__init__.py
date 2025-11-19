import os
import fcntl
from .no_asyncio_nats import connect as _connect_impl


def connect(address, options=None):
    return _connect_impl(address, os.eventfd_read, options)

def gevent_connect(address, options=None):
    return _connect_impl(address, gevent_eventfd_read, options)

def gevent_eventfd_read(fd: int):
    import gevent.socket

    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    while True: 
        try:                                                                         
            os.eventfd_read(fd)
        except OSError as error:
            if error.errno == errno.EAGAIN:
                gevent.socket.wait_read(fd)                                          
            else:
                raise
        else:                                                                        
            break
