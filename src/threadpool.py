# -*- coding: UTF-8 -*-
"""Easy to use object-oriented thread pool framework.

A thread pool is an object that maintains a pool of worker threads to perform
time consuming operations in parallel. It assigns jobs to the threads
by putting them in a work request queue, where they are picked up by the
next available thread. This then performs the requested operation in the
background and puts the results in another queue.

The thread pool object can then collect the results from all threads from
this queue as soon as they become available or after all threads have
finished their work. It's also possible, to define callbacks to handle
each result as it comes in.

The basic concept and some code was taken from the book "Python in a Nutshell,
2nd edition" by Alex Martelli, O'Reilly 2006, ISBN 0-596-10046-9, from section
14.5 "Threaded Program Architecture". I wrapped the main program logic in the
ThreadPool class, added the WorkRequest class and the callback system and
tweaked the code here and there. Kudos also to Florent Aide for the exception
handling mechanism.

Basic usage::

    >>> pool = ThreadPool(poolsize)
    >>> requests = make_requests(some_callable, list_of_args, callback)
    >>> [pool.put_request(req) for req in requests]
    >>> pool.wait()

See the end of the module code for a brief, annotated usage example.

Website : http://chrisarndt.de/projects/threadpool/

"""
__docformat__ = "restructuredtext en"

__all__ = [    # TODO: usage of "__all__" and some other similar variables
    'make_requests',
    'NoResultsPending',
    'NoWorkersAvailable',
    'ThreadPool',
    'WorkRequest',
    'WorkerThread'
]

__author__ = "Christopher Arndt"
__version__ = '1.3.2'
__license__ = "MIT license"


# standard library modules
import sys
import threading
import traceback

try:
    import Queue            # Python 2
except ImportError:
    import queue as Queue   # Python 3


# exceptions
class NoResultsPending(Exception):
    """All work requests have been processed."""
    pass

class NoWorkersAvailable(Exception):
    """No worker threads available to process remaining requests."""
    pass


# internal module helper functions
def _handle_thread_exception(request, exc_info):
    """Default exception handler callback function.

    This just prints the exception info via ``traceback.print_exception``.

    """
    traceback.print_exception(*exc_info)    # TODO: usage of "*"


# utility functions
def make_requests(callable_, args_list, callback=None, exc_callback=_handle_thread_exception):
    """Create several work requests for same callable with different arguments.

    Convenience function for creating several work requests for the same
    callable where each invocation of the callable receives different values
    for its arguments.

    ``args_list`` contains the parameters for each invocation of callable.
    Each item in ``args_list`` should be either a 2-item tuple of the list of
    positional arguments and a dictionary of keyword arguments or a single,
    non-tuple argument.

    See docstring for ``WorkRequest`` for info on ``callback`` and
    ``exc_callback``.
    
    """
    requests = []
    for item in args_list:
        if isinstance(item, tuple):
            requests.append(
                # item[0]: the list of positional arguments;    item[1]: a dictionary of keyword arguments.
                WorkRequest(callable_, item[0], item[1], callback=callback, exc_callback=exc_callback)
            )
        else:
            requests.append(
                # item: a single, non-tuple argument.
                WorkRequest(callable_, [item], None, callback=callback, exc_callback=exc_callback)
            )
    return requests


# classes
class WorkerThread(threading.Thread):
    """Background thread connected to the requests/results queues.

    A worker thread sits in the background and picks up work requests from
    one queue and puts the results in another until it is dismissed.

    """

    def __init__(self, requests_queue, results_queue, poll_timeout=5, **kwds):
        """Set up thread in daemonic mode and start it immediately.

        ``requests_queue`` and ``results_queue`` are instances of
        ``Queue.Queue`` passed by the ``ThreadPool`` class when it creates a
        new worker thread.

        """
        threading.Thread.__init__(self, **kwds)
        self.daemon = True    # "self.setDaemon(1)" is old
        self._requests_queue = requests_queue
        self._results_queue = results_queue
        self._poll_timeout = poll_timeout
        self._dismissed = threading.Event()
        self.start()

    def run(self):
        """Repeatedly process the job queue until told to exit."""
        while 1:
            if self._dismissed.is_set():    # "if self._dismissed.isSet():" is also OK
                # The worker thread is dismissed, break out of loop
                break
            # get next work request. If we don't get a new request from the
            # queue after self._poll_timout seconds, we jump to the start of
            # the while loop again, to give the thread a chance to exit.
            try:
                """
                `self._poll_timeout` sets the interval in seconds (int or float) for how
                often threads should check whether they are dismissed, while waiting for
                requests.
                """
                request = self._requests_queue.get(block=True, timeout=self._poll_timeout)
            except Queue.Empty:
                continue
            else:
                if self._dismissed.is_set():    # if self._dismissed.isSet():
                    # The worker thread is dismissed, put back request in queue and exit loop
                    self._requests_queue.put(request)
                    break
                try:
                    result = request.callable(*request.args, **request.kwds)
                    self._results_queue.put((request, result))
                except:
                    request.exception = True
                    self._results_queue.put((request, sys.exc_info()))

    def dismiss(self):
        """Set a flag to tell the thread to exit when done with current job."""
        self._dismissed.set()


class WorkRequest:
    """A request to execute a callable for putting in the request queue later.

    See the module function ``make_requests`` for the common case
    where you want to build several ``WorkRequest`` objects for the same
    callable but with different arguments for each call.

    """

    def __init__(self, callable_, args=None, kwds=None, request_id=None,
            callback=None, exc_callback=_handle_thread_exception):
        """Create a work request for a callable and attach callbacks.

        A work request consists of a `callable` to be executed by a
        worker thread, a list of positional arguments(`args`), a dictionary
        of keyword arguments(`kwds`).

        A ``callback`` function can be specified, that is called when the
        results of the request are picked up from the result queue. It must
        accept two anonymous arguments, the ``WorkRequest`` object and the
        results of the callable, in that order. If you want to pass additional
        information to the callback, just stick it on the request object.

        You can also give custom callback for when an exception occurs with
        the ``exc_callback`` keyword parameter. It should also accept two
        anonymous arguments, the ``WorkRequest`` and a tuple with the exception
        details as returned by ``sys.exc_info()``. The default implementation
        of this callback just prints the exception info via
        ``traceback.print_exception``. If you want no exception handler
        callback, just pass in ``None``.

        ``request_id``, if given, must be hashable since it is used by
        ``ThreadPool`` object to store the results of that work request in a
        dictionary. It defaults to the return value of ``id(self)``.

        """
        if request_id is None:
            self.request_id = id(self)
        else:
            try:
                self.request_id = hash(request_id)    # TODO: what is hashable?
            except TypeError:
                raise TypeError("request_id must be hashable.")
        self.exception = False
        self.callback = callback
        self.exc_callback = exc_callback
        self.callable = callable_
        self.args = args or []
        self.kwds = kwds or {}

    def __str__(self):    # TODO: __str__()?
        # return "<WorkRequest id=%s args=%r kwargs=%r exception=%s>" % \
        #     (self.request_id, self.args, self.kwds, self.exception)
        return "<WorkRequest id={0} args={1} kwargs={2} exception={3}>".format(
            self.request_id, self.args, self.kwds, self.exception)


class ThreadPool:
    """A thread pool, distributing work requests and collecting results.

    See the module docstring for more information.

    """

    def __init__(self, num_workers, q_size=0, res_q_size=0, poll_timeout=5):
        """Set up the thread pool and start num_workers worker threads.

        ``num_workers`` is the number of worker threads to start initially.

        If ``q_size > 0`` the size of the *request queue* is limited and the
        thread pool blocks when the queue is full and it tries to put more work
        requests in it (see ``put_request`` method), unless you also use a
        positive ``timeout`` value for ``put_request``.

        If ``res_q_size > 0`` the size of the *results queue* is limited and the
        worker threads will block when the queue is full and they try to put
        new results in it.

        WARNING:
            If you set both ``q_size`` and ``res_q_size`` to ``!= 0`` there is
            the possibilty of a deadlock, when the results queue is not pulled
            regularly and too many jobs are put in the work requests queue.
            To prevent this, always set ``timeout > 0`` when calling
            ``ThreadPool.put_request()`` and catch ``Queue.Full`` exceptions.

        """
        self._requests_queue = Queue.Queue(q_size)
        self._results_queue = Queue.Queue(res_q_size)
        self.workers = []    # list of WorkerThread.
        self.dismissed_workers = []
        self.work_requests = {}
        self.create_workers(num_workers, poll_timeout)

    def create_workers(self, num_workers, poll_timeout=5):
        """Add `num_workers` worker threads to the pool.

        ``poll_timeout`` sets the interval in seconds (int or float) for how
        often threads should check whether they are dismissed, while waiting for
        requests.

        """
        for i in range(num_workers):
            # WorkerThread: def __init__(self,   requests_queue,     results_queue,      poll_timeout=5, **kwds):
            self.workers.append(WorkerThread(self._requests_queue, self._results_queue, poll_timeout=poll_timeout))

    def dismiss_workers(self, num_workers, do_join=False):
        """Tell num_workers worker threads to quit after their current task."""
        dismiss_list = []
        for i in range(min(num_workers, len(self.workers))):
            worker = self.workers.pop()
            worker.dismiss()
            dismiss_list.append(worker)

        if do_join:
            for worker in dismiss_list:
                worker.join()
        else:
            self.dismissed_workers.extend(dismiss_list)

    def join_all_dismissed_workers(self):
        """Perform Thread.join() on all worker threads that have been dismissed.
        """
        for worker in self.dismissed_workers:
            worker.join()
        self.dismissed_workers = []

    def put_request(self, request, block=True, timeout=None):
        """Put work request into work queue and save its id for later."""
        assert isinstance(request, WorkRequest)
        # don't reuse old work requests
        assert not getattr(request, 'exception', None)
        self._requests_queue.put(request, block, timeout)
        self.work_requests[request.request_id] = request

    def poll(self, block=False):
        """Process any new results in the queue."""
        while 1:
            # still results pending?
            if not self.work_requests:
                raise NoResultsPending
            # are there still workers to process remaining requests?
            elif block and not self.workers:
                raise NoWorkersAvailable
            try:
                # get back next results    # Queue.get(): Remove and return an item from the queue.
                request, result = self._results_queue.get(block=block)
                """
                # has an exception occured?
                if request.exception and request.exc_callback:
                    request.exc_callback(request, result)
                # hand results to callback, if any
                if request.callback and not (request.exception and request.exc_callback):
                    request.callback(request, result)
                """
                if request.exception and request.exc_callback:    # has an exception occured?
                    request.exc_callback(request, result)
                elif request.callback:    # hand results to callback, if any
                    request.callback(request, result)
                del self.work_requests[request.request_id]
            except Queue.Empty:
                break

    def wait(self):
        """Wait for results, blocking until all have arrived."""
        while 1:
            try:
                self.poll(True)
            except NoResultsPending:
                break


################
# USAGE EXAMPLE
################

if __name__ == '__main__':
    import random
    import time

    # the work the threads will have to do (rather trivial in our example)
    def do_something(data):
        time.sleep(random.randint(1,5))
        result = round(random.random() * data, 5)
        # just to show off, we throw an exception once in a while
        if result > 5:
            raise RuntimeError("Something extraordinary happened!")
        return result

    # this will be called each time a result is available
    def print_result(request, result):
        print("**** Result from request #%s: %r" % (request.request_id, result))

    # this will be called when an exception occurs within a thread
    # this example exception handler does little more than the default handler
    def handle_exception(request, exc_info):
        if not isinstance(exc_info, tuple):
            # Something is seriously wrong...
            print(request)
            print(exc_info)
            raise SystemExit
        print("**** Exception occured in request #%s: %s" % \
          (request.request_id, exc_info))

    # assemble the arguments for each job to a list...
    data = [random.randint(1,10) for i in range(20)]
    # ... and build a WorkRequest object for each item in data
    requests = make_requests(do_something, data, print_result, handle_exception)
    # to use the default exception handler, uncomment next line and comment out
    # the preceding one.
    #requests = make_requests(do_something, data, print_result)

    # or the other form of args_lists accepted by make_requests: ((,), {})
    data = [((random.randint(1,10),), {}) for i in range(20)]
    requests.extend(
        make_requests(do_something, data, print_result, handle_exception)
        #make_requests(do_something, data, print_result)
        # to use the default exception handler, uncomment next line and comment
        # out the preceding one.
    )

    # we create a pool of 3 worker threads
    print("Creating thread pool with 3 worker threads.")
    main = ThreadPool(3)

    # then we put the work requests in the queue...
    for req in requests:
        main.put_request(req)
        print("Work request #%s added." % req.request_id)
    # or shorter:
    # [main.put_request(req) for req in requests]

    # ...and wait for the results to arrive in the result queue
    # by using ThreadPool.wait(). This would block until results for
    # all work requests have arrived:
    # main.wait()

    # instead we can poll for results while doing something else:
    i = 0
    while True:
        try:
            time.sleep(0.5)
            main.poll()
            print("Main thread working...")
            print("(active worker threads: %i)" % (threading.activeCount()-1, ))
            if i == 10:
                print("**** Adding 3 more worker threads...")
                main.create_workers(3)
            if i == 20:
                print("**** Dismissing 2 worker threads...")
                main.dismiss_workers(2)
            i += 1
        except KeyboardInterrupt:
            print("**** Interrupted!")
            break
        except NoResultsPending:
            print("**** No pending results.")
            break
    if main.dismissed_workers:
        print("Joining all dismissed worker threads...")
        main.join_all_dismissed_workers()
