import sys
import json
import time
import datetime
import Queue
import subprocess
import threading

exitFlag = threading.Event()

# sybil_cmd should be read as the command line
if len(sys.argv) < 1:
    print "Usage is: camonitor_to_sybil.py /path/to/sybil"
    sys.exit(0)

sybil_cmd=sys.argv[1:]

queue = Queue.Queue()

def dump_data():
    sybil_cmd.extend(["ingest", "-table", "camonitor"])
    samples = []
    while True:
        if exitFlag.isSet():
            sys.exit(0)
            break

        sample = queue.get()
        samples = [sample]
        while not queue.empty():
            sample = queue.get()
            samples.append(sample)

        pop = subprocess.Popen(sybil_cmd, stdin=subprocess.PIPE)
        pop.communicate("\n".join(map(lambda s: json.dumps(s), samples)))
        print "SENT SAMPLES (%i)" % (len(samples))

        # sleep for a second, just for fun
        time.sleep(1)


def read_data():
    while True:
        line = sys.stdin.readline()
        tokens = line.split()
        if len(tokens) < 4:
            continue

        ts = tokens[1]
        clock = tokens[2].split(".")[0]
        date_string = "%Y-%m-%d %H:%M:%S"
        full = tokens[1] + " " + clock
        parsed = datetime.datetime.strptime(full, date_string)
        from_epoch = (parsed - datetime.datetime(1970,1,1)).total_seconds()
        sample = { "value" : int(tokens[3]), "name" : tokens[0], "timestamp": int(from_epoch)}
        queue.put(sample)


dump_thread = threading.Thread(target=dump_data)
dump_thread.start()

try:
    # the read_data happens in main thread
    read_data()
except KeyboardInterrupt:
    print("Hit keyboard interrupt, Quitting")
    exitFlag.set()
    queue.put(None)
    sys.exit(0)
