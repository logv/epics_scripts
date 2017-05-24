import sys
import json
import time
from datetime import datetime
import Queue
import subprocess
import threading
import epics

## CONFIG 
SYBIL_BIN="/home/okay/tonka/src/snorkel.snap/Snorkel-0.0.20-x86_64.AppImage backend"
SLEEP_INTERVAL=1
DEBUG=False

## CODE BELOW
exitFlag = threading.Event()
sybil_cmd=SYBIL_BIN.split()
queue = Queue.Queue()

class PVMonitor(object):
    # TODO: grab metadata for this PV to integrate into samples when sending
    def __init__(self, name):
        self.name = name

    def run(self):
        print "STARTING READ THREAD", self.name
        self.pv = epics.pv.PV(self.name, callback=self.pv_update, auto_monitor=True)

    def pv_update(self, *args, **kwargs):
        ts = kwargs["timestamp"]
        sample = { 
            "value" : int(kwargs["value"]), 
            "name" : self.name, 
            "timestamp": int(ts)}
        queue.put(sample)
        
class MultiMonitor(object):
    def __init__(self, args):
        self.pv = []
        for arg in args:
            self.pv.append(PVMonitor(arg))

    def dump_data(self):
        sybil_cmd.extend(["ingest", "-table", "camonitor"])
        samples = []
        it = 0
        while True:
            it += 1
            if exitFlag.isSet():
                sys.exit(0)
                break

            try:
                sample = queue.get(True, SLEEP_INTERVAL)
            except Exception, e:
                continue

            samples = [sample]
            while not queue.empty():
                sample = queue.get()
                samples.append(sample)

            pop = subprocess.Popen(sybil_cmd, stdin=subprocess.PIPE)
            pop.communicate("\n".join(map(lambda s: json.dumps(s), samples)))
            print "SENT SAMPLES (%i)" % (len(samples)), "ON ITER", it
            if DEBUG:
                print " VARS:", map(lambda w: w['name'], samples)

            # sleep for a second, just for fun
            time.sleep(SLEEP_INTERVAL)

    def run(self):
        dump_thread = threading.Thread(target=self.dump_data)
        dump_thread.start()

        for pv in self.pv:
            pv.run()

        i = 0
        while True:
            try:
                # the read_data happens in main thread
                time.sleep(SLEEP_INTERVAL)
                i += 1
            except KeyboardInterrupt:
                print("Hit keyboard interrupt, Quitting")
                exitFlag.set()
                queue.put(None)
                sys.exit(0)


if __name__ == "__main__":
    mm = MultiMonitor(["okay:calc1", "okay:calc2"])
    mm.run()
