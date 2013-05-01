import inotifyx as inotify

from tornado import web
from tornadio2 import SocketConnection, TornadioRouter, SocketServer, event

from threading import Thread
import base64
import os, time, sys, getopt
from subprocess import Popen
from Queue import Queue
import signal
import atexit
import cgi
import requests
import simplejson as json
import threading
import logging


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
ONLINE_MODELS_URL='http://dev.seeme.com:6100/onlinemodels'
PID_FILE='/tmp/DEV_ffmpeg_websocket_server.pid'
PORT=8022
FLASH_PORT=10842

# global setting
MAX_FPS = 15
MIN_FPS = 1
INC_DEC_FACTOR = 1
BOGGED_FACTOR = 0.7 # used to send to client more fps than currently
JPEG_QUALITY = 1  # 1-high, 31-low, never go above 13
JPEG_SIZE = '320x240'
CLEAN_INTERVAL = 5 
FNULL = open('/dev/null', 'w')
NAMESPACE='/vid'

class StreamDumper(object):
    def __init__(self):
        self.processes = {}

    def start_dump(self, model, wait=0):
        if model in self.processes:
	     if self.processes[model].poll() == None: # if ffmpeg process is running than do not start
                 logging.debug("ignoring...")
                 return
        command = ['/usr/local/bin/ffmpeg', '-analyzeduration', '0', '-tune', 'zerolatency',
             '-shortest', '-xerror', '-indexmem', '1000', '-rtbufsize', '1000', '-max_alloc', '2000000',
             '-i', 'rtmp://%s/%s/%s/%s_%s live=1' % (STREAM_SERVER, STREAM_USER, model, model, model),
             '-an', '-r', str(MAX_FPS), '-s', JPEG_SIZE, '-threads', '1', '-q:v', str(JPEG_QUALITY),
              model + '_img%d.jpg']
	logging.debug(" ".join(command))
	gevent.sleep(wait) # wait a while for stream to start
        p = Popen(command, cwd=PIC_PATH, stdout=FNULL, stderr=FNULL, close_fds=True)
        self.processes[model] = p

    def stop_dump(self, model):
        if model in self.processes:
            p = self.processes[model]
            try:
                if not p.poll():
                     p.kill()
                     p.wait()
            finally:
                del self.processes[model]

class ImgConnection(SocketConnection):
    @event('open')    
    def open(self):
        with sem: web_sockets.append(self)
        self.client_fps = 0             # actual fps received from client
        self.fps = MIN_FPS              # currently serving fps
        self.fps_counter = self.fps      # fps counter for sending
        self.model = None
        self.loop_running = True
        Thread(target=self.fps_loop).start()     # spawns fps control
        return True
        
    @event('close')
    def close(self):
        if self in web_sockets:
            with sem: web_sockets.remove(self)
        logging.debug("RECEIVED DISCONNECT")
        self.loop_running = False

    @event
    def join(self, model):
        logging.debug("JOIN: %s" % model)
        self.model = model
        stream_dumper.start_dump(self.model)
    
    @event
    def heartbeat(self, fps):
        #logging.debug "GOT: %s HAVE: %s" % (fps, getattr(self, 'fps', 0))
        self.client_fps = fps
    
    def fps_loop(self):
        logging.debug("event loop spawned")
        def fps_control(self):
            if self.client_fps >= self.fps and self.fps < MAX_FPS:            
                self.fps += INC_DEC_FACTOR
                #logging.debug "INC TO: ", self.fps
            elif self.client_fps < (self.fps * BOGGED_FACTOR) and self.fps > MIN_FPS:   
                self.fps -= INC_DEC_FACTOR
                #logging.debug "DEC TO: ", self.fps
            self.session['fps_counter'] = self.fps
            self.heartbeat = False
        while self.loop_running:
            fps_control(self)
            time.sleep(1)
    
    def send(self, msg):
        if self.fps_counter > 0:
            #data = base64.encodestring(msg)
            self.emit('img', {'b64jpeg': base64.encodestring(msg)})
            self.fps_counter -= 1

class Application(object):
    def __init__(self):
        self.buffer = []

    def __call__(self, environ, start_response):
        path = environ['PATH_INFO'].strip('/')

        if path.startswith("socket.io"):
            socketio_manage(environ, {NAMESPACE: SocketHandler})
            return
        if path.startswith("stats"):
            return respond(stats(), start_response)
        if path.startswith("modelstatus"):
            post = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ, keep_blank_values=True)
            status = post.getfirst('status', '')
            model = post.getfirst('userName', '')
            if not model: return respond('invalid model name', start_response)
            logging.debug("MODEL %s STATUS: %s" % (model, status))
            if status == 'start':
                  Thread(target=stream_dumper.start_dump, args=(model, 2)).start()
            else:
                  Thread(target=stream_dumper.stop_dump, target=(model,)).start()
            return respond('ok', start_response)
        return not_found(start_response)

def respond(message, start_response):
    start_response('200', [])
    return [message]

def not_found(start_response):
    start_response('404 Not Found', [])
    return ['<h1>Not Found</h1>']

def stats():
    return """<table>
<tr><td>Active sessions</td><td>%d</td></tr>
<tr><td>Active models</td><td>%d</td></tr>
<tr><td>Active ffmpeg processes</td><td>%d</td></tr>
<tr><td>Image queue length</td><td>%d</td></tr>
</table>""" % (len(server.sockets),
               len(stream_dumper.processes),
               len([p for p in stream_dumper.processes.values() if p.poll() == None]),
               q.qsize())

def send_img():
     while True:
         img = None
         with sem:
            for ws in web_sockets:
               if event.name.lower().startswith(ws.model.lower() + "_img"):
                  if img == None:
                        with open(PIC_PATH + event.name, 'rb') as f:
                             img = f.read()          
                  ws.send(img)

def event_producer(fd, q):
    while True:
        events = inotify.get_events(fd)
        for event in events:           
            q.put(event)

'''repetitively cleans all file from the specified path older than specified interval'''
def file_cleanup(path, interval, server):
    while True:
        try: #this loop must never stop
           now = time.time()
           count = 0
           for f in os.listdir(PIC_PATH):
               f = os.path.join(PIC_PATH, f)
               if os.stat(f).st_mtime < now - interval:
                   os.remove(f)
                   count += 1
           sess_count = 0
           for sessid, socket in server.sockets.iteritems():
              if not socket.connected:
                   socket.kill(detach=True)
                   sess_count += 1
           if DEBUG:
               print("Active models: %d" % len(stream_dumper.processes))
               print("cleanned: %d files" % count)
               print("cleanned: %d sessions" % sess_count)
           # if queue is full then shutdown (hope some monitoring process will restart it)
           # no point keeping it online
           if q.qsize() == 1000:
               exit_cleanup()
           time.sleep(interval)
        except Exception as e: print "EXCEPTION IN CLEAN LOOP: %s" % e

"""kill all ffmpeg before exit"""    
def exit_cleanup(signumi=None, frame=None):
    logging.debug("killing all ffmpeg processes before exit...")
    os.remove(PID_FILE)
    for model in stream_dumper.processes:
        try:
           p = stream_dumper.processes[model]
           p.kill()
           p.wait()
        except: pass
    sys.exit(0)

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
            ONLINE_MODELS_URL='http://www.seeme.com/onlinemodels'
            PID_FILE='/tmp/LIVE_ffmpeg_websocket_server.pid'
            PORT=8023
            FLASH_PORT=10843
    else:
        assert False, "unhandled option"

sem = threading.Lock()
web_sockets = []
q = Queue(1000)
fd = inotify.init()
inotify.add_watch(fd, PIC_PATH, inotify.IN_CREATE)
stream_dumper = StreamDumper()
Thread(target=event_producer, args=(fd, q).start()
Thread(target=send_img, args=(server,)).start()
Thread(target=file_cleanup, args=(PIC_PATH, CLEAN_INTERVAL, server)).start()


signal.signal(signal.SIGTERM, exit_cleanup)
signal.signal(signal.SIGINT , exit_cleanup) 
signal.signal(signal.SIGQUIT, exit_cleanup)
atexit.register(exit_cleanup)

logging.debug("writing pid file: %s" % PID_FILE)
with open(PID_FILE, 'w') as f:
    f.write(str(os.getpid()))

# start ffmpeg processes for online models
r = requests.get(ONLINE_MODELS_URL)
if r.status_code == 200:
    logging.debug("starting initial ffmpeg processes for: %s" % r.content)
    online_model_list = json.loads(r.content)
    for model in online_model_list:
	stream_dumper.start_dump(model)


# Create tornadio router
ImgRouter = TornadioRouter(ImgConnection)

# Create socket application
application = web.Application(
    ImgRouter.urls,
    flash_policy_port = FLASH_PORT,
    flash_policy_file = op.join(ROOT, 'flashpolicy.xml'),
    socket_io_port = PORT
)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG if DEBUG else logging.INFO)
    # Create and start tornadio server
    SocketServer(application)


