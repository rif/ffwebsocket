from gevent import monkey; monkey.patch_all()
import gevent

from socketio import socketio_manage
from socketio.server import SocketIOServer
from socketio.namespace import BaseNamespace

import gevent_inotifyx as inotify
from gevent.queue import Queue
from gevent.coros import Semaphore

from base64 import b64encode
import os, time, sys, getopt
from subprocess import Popen
import signal
import atexit

DEBUG = False
"""
origins
Chicago: ded817 ip: 66.254.120.102
Amsterdam: ded1133 ip: 31.192.112.170
edges
Chicago: ded457 ip (eth0): 216.18.184.22 (eth1) 10.21.51.22
Amsterdam: ded1178 ip: 31.192.113.234
"""
# dev settings. NOTE! live setting overwritten in main
STREAM_SERVER='66.254.119.93:1935'
STREAM_USER='SeemeHostLiveOrigin'
PIC_PATH = '/srv/DEV_model_imgs_ramfs/'
PORT=8023
FLASH_PORT=10843

# global setting
MAX_FPS = 15
MIN_FPS = 1
INC_DEC_FACTOR = 1
BOGGED_FACTOR = 0.7 # used to send to client more fps than currently
JPEG_QUALITY = 1  # 1-high, 31-low, never go above 13
JPEG_SIZE = '320x240'
CLEAN_INTERVAL = 5 
FNULL = open('/dev/null', 'w')

class StreamDumper(object):
    def __init__(self):
        self.processes = {}

    def start_dump(self, model):
        if model in self.processes:
             p = self.processes[model]
             p.kill()
        p = Popen(['/usr/local/bin/ffmpeg', '-analyzeduration', '0', '-tune', 'zerolatency',
             '-i', 'rtmp://%s/%s/%s/%s_%s live=1' % (STREAM_SERVER, STREAM_USER, model, model, model),
             '-an', '-r', str(MAX_FPS), '-s', JPEG_SIZE, '-q:v', str(JPEG_QUALITY),
              model + '_img%d.jpg'], cwd=PIC_PATH, stdout=FNULL, stderr=FNULL)
        self.processes[model] = p

    def stop_dump(self, model):
        if model in self.processes:
            p = self.processes[model]
            p.kill()

class SocketHandler(BaseNamespace):    
    def recv_connect(self):
        sem.acquire()
        web_sockets.append(self)
        sem.release()
        self.loop_on = True             # lope stop condition
        self.client_fps = 0             # actual fps received from client
        self.fps = MIN_FPS              # currently serving fps
        self.fps_counter = self.fps      # fps counter for sending
        self.model = ''
        gevent.spawn(self.fps_loop)     # spawns fps control

    def recv_disconnect(self):        
        if self in web_sockets:
            sem.acquire()
            web_sockets.remove(self)
            sem.release()
        self.loop_on = False
    
    def fps_loop(self):
        def fps_control(self):        
            if self.client_fps >= self.fps and self.fps < MAX_FPS:            
                self.fps += INC_DEC_FACTOR
                if DEBUG: print "INC TO: ", self.fps
            elif self.client_fps < (self.fps * BOGGED_FACTOR) and self.fps > MIN_FPS:   
                self.fps -= INC_DEC_FACTOR
                if DEBUG: print "DEC TO: ", self.fps
            self.fps_counter = self.fps
            self.heartbeat = False
            gevent.sleep(1)
        while self.loop_on:
            fps_control(self)
    
    def on_set_model(self, msg):
        if DEBUG: print "MODEL REQUEST: ", msg['model']
        self.model = msg['model']
    
    def on_heartbeat(self, msg):
        if DEBUG: print "GOT: %s HAVE: %s" % (msg['fps'], getattr(self, 'fps', 0))
        self.client_fps = msg['fps']
    
    def send(self, msg):
        if self.fps_counter > 0:
            self.emit('img', {'b64jpeg': msg})
            self.fps_counter -= 1


class Application(object):
    def __init__(self):
        self.buffer = []

    def __call__(self, environ, start_response):
        path = environ['PATH_INFO'].strip('/')
        query = environ['QUERY_STRING']

        if path.startswith("socket.io"):
            socketio_manage(environ, {'/vid': SocketHandler})
            return
        if path.startswith("start"):
            if DEBUG: print "MODEL ONLINE: ", query
            stream_dumper.start_dump(query)
            return ok(start_response)
        if path.startswith("stop"):
            if DEBUG: print "MODEL OFFLINE: ", query
            stream_dumper.stop_dump(query)
            return ok(start_response)
        return not_found(start_response)


def ok(start_response):
    start_response('200', [])
    return ['ok']

def not_found(start_response):
    start_response('404 Not Found', [])
    return ['<h1>Not Found</h1>']

def send_img():
    while True:
        event = q.get()
        sem.acquire()
        path = PIC_PATH + event.name
        with open(path, 'rb') as f:
            data = f.read()
        for ws in web_sockets:                        
            if event.name.startswith(ws.model + "_img"):
                 gevent.spawn(ws.send, b64encode(data))
        sem.release()

def event_producer(fd, q):
    while True:
        events = inotify.get_events(fd)
        for event in events:           
            q.put(event)

'''repetitively cleans all file from the specified path older than specified interval'''
def cleanup(path, interval):
    while True:
        now = time.time()
        for f in os.listdir(PIC_PATH):
            f = os.path.join(PIC_PATH, f)
            if os.stat(f).st_mtime < now - interval:
                if os.path.isfile(f): os.remove(f)
        gevent.sleep(interval)

"""kill all ffmpeg before exit"""    
def sig_handler(signum, frame):
    kill_ffmpeg_on_exit()
    sys.exit(0)

def kill_ffmpeg_on_exit():
    if DEBUG: print("killing all ffmpeg processes before exit...")
    for model,p in stream_dumper.processes:
        p.kill()

try:
    opts, args = getopt.getopt(sys.argv[1:], "dr:", ["debug", "run="])
except getopt.GetoptError as err:
    # print help information and exit:
    print str(err) # will print something like "option -a not recognized"
    print("USEAGE:\n\t -r/--run=live|dev \n\t -d/--debug for debug")
    sys.exit(2)
for o, a in opts:
    if o in ("-d", "--debug"):
        DEBUG = True
    elif o in ("-r", "--run"):
        if a == 'live':
            STREAM_SERVER='216.18.184.22:1935'
            STREAM_USER='UserEdge'
            PIC_PATH = '/srv/LIVE_model_imgs_ramfs/'
            PORT=8022
            FLASH_PORT=10842
    else:
        assert False, "unhandled option"

sem = Semaphore()
web_sockets = []
q = Queue()
fd = inotify.init()
inotify.add_watch(fd, PIC_PATH, inotify.IN_CREATE)
stream_dumper = StreamDumper()
gevent.spawn(cleanup, PIC_PATH, CLEAN_INTERVAL)
gevent.spawn(event_producer, fd, q)
gevent.spawn(send_img)

signal.signal(signal.SIGTERM, sig_handler) 
signal.signal(signal.SIGINT , sig_handler) 
gevent.signal(signal.SIGQUIT, sig_handler)
atexit.register(kill_ffmpeg_on_exit)

print('Listening on port http://0.0.0.0:%s and on port %s (flash policy server)'% (PORT, FLASH_PORT))
SocketIOServer(('0.0.0.0', PORT), Application(),
               resource="socket.io", policy_server=True,
               policy_listener=('0.0.0.0', FLASH_PORT)).serve_forever()

