#!/usr/bin/python

'''
worker_thread.py - Implementation of worker thread

'''

import logging
import threading
import time
try:
    import Queue
except ImportError:
    import queue as Queue


logger = logging.getLogger('')

class WorkerThread(threading.Thread):

    def __init__(self, job_q, result_q):
        super(WorkerThread, self).__init__()
        self._job_q = job_q
        self._result_q = result_q

    def run(self):
        #curThdName = threading.current_thread().name
        while True:
            try:
                job = self._job_q.get(True,1)
            except Queue.Empty:  # Exit the worker if Q empty
                time.sleep(.050)  #sleep 50 miliseconds
                continue
            job.execute()
            #self._result_q.put(job)
            self._job_q.task_done()
        return True
