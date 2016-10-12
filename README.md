# redismq

A *clojure* library for Redis-based message queues.

Featuring:

* Retries
* Failure backlog, with limits on depth
* Limits on queue depth
* JSON serialization

## Usage

````
;; define our test queue
(def queue (make-queue "test" {:host "localhost}))

;; put some events on the queue
(enqueue queue "Truly")
(enqueue queue "I")
(enqueue queue "am")
(enqueue queue "awesome")

;; we'll pull one off the queue
(consume queue (fn [event] (print "First Event: " event)))

;; a reaper will check the processing queue, and clean up dead events
;; we have it check every 10 seconds
(queue-reaper 10000 queue)

;; This starts up a worker that will wait 1 second between events, and
;; will take at most 1 second to process the event.  If the event
;; takes more than one second to process, it will timeout and it will
;; count a sa "failure" for the event.
(queue-worker queue
  (fn [event]
    (print "Event: " event))
  :delay 1000
  :timeout 1000))
````

## License

Copyright © 2016 Cisco

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.