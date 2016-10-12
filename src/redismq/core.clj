(ns redismq.core
  (:require [cheshire.core :as json]
            [clojure.tools.logging :refer [warn error fatal info]]
            [taoensso.carmine :as car]
            [taoensso.carmine.connections :as connections]))

;; ----------------------------------------
;; queue state

(defonce queue-reapers
  #_"An atom containing a list of all queue reapers in the current system."
  (atom ()))


(defonce queue-workers
  #_"An atom containing a list of all queue workers in current system."
  (atom ()))



;; protocol to convert the data into a form suiable for serialization
;; to and from JSON.
(defprotocol Serialize
  (deserialize [converter ext-value])
  (serialize [converter int-value]))

(defonce identity-serializer
  (reify Serialize
    (deserialize [_ ext-value] ext-value)
    (serialize [_ int-value] int-value)))

;; ----------------------------------------

;; A minimalist message queue mechanism

(defn make-queue
  "Returns a queue object, used in the rest of the MQ api."
  ([name conn-spec]
     (make-queue name conn-spec {}))
  ([name conn-spec {:keys [conn-pool max-depth retries
                           serializer failure-max-depth]
                    :or {failure-max-depth 0
                         retries 5
                         serializer identity-serializer}}]
     (merge {:name name
             :processing-key (str name "-processing")
             :spec (connections/conn-spec conn-spec)
             :pool (or conn-pool (connections/conn-pool {}))
             :max-depth max-depth
             :retries retries
             :serializer serializer}
            (when (< 0 failure-max-depth)
              {:failure-key (str name "-failures")
               :failure-max-depth failure-max-depth}))))

(defn wrap-obj
  "Prepares a data value for the queue, returning a serialized map containing:
  * :ts -- A unix timestamp
  * :id -- A UUID
  * :data -- The original datum"
  [q obj]
  (let [data (serialize (:serializer q) obj)]
    (json/generate-string
     {:id (java.util.UUID/randomUUID)
      :ts (System/currentTimeMillis)
      :data data})))

(defn unwrap-obj
  "Returns a queue message, a map containing:
  * :ts -- A unix timestamp
  * :id -- A UUID
  * :q -- The queue the message came from
  * :data -- The original datum"
  [q obj]
  (let [o (json/parse-string obj true)
        data (deserialize (:serializer q) (:data o))]
    (assoc o
      :queue q
      :data data)))

(defn enqueue-message [queue message-text]
  (let [queue-key (:name queue)
        item-count (car/wcar queue
                     (car/lpush queue-key message-text))
        max-depth (:max-depth queue)]
    (when (and max-depth
               (< max-depth item-count))
      (car/wcar queue
        (car/ltrim queue-key 0 (dec max-depth))))))

(defn enqueue [queue obj]
  (when (empty? (:name queue))
    (throw (RuntimeException. (str "Queue requires :name - " queue))))
  (enqueue-message queue (wrap-obj queue obj)))

(defn current-queue
  ([queue]
     (current-queue queue -1))
  ([queue limit]
     (map #(unwrap-obj queue %)
          (car/wcar queue
            (car/lrange (:name queue) 0 limit)))))

(defn current-depth [queue]
  (car/wcar queue
    (car/llen (:name queue))))

(defn current-processing
  "Returns the current set of jobs being processed, in their serialized form."
  ([queue]
     (current-processing queue -1))
  ([queue limit]
     (car/wcar queue
               (car/lrange (:processing-key queue) 0 limit))))

(defn failed-messages [queue]
  (when (:failure-key queue)
    (map #(unwrap-obj queue %)
         (car/wcar queue
           (car/lrange (:failure-key queue) 0 -1)))))

(defn flush-queue [queue]
  (car/wcar queue
    (car/del (:name queue)))
  (car/wcar queue
    (car/del (:processing-key queue)))
  (when-let [failure-key (:failure-key queue)]
    (car/wcar queue
      (car/del failure-key))))

(defn consume
  "Takes a message from the queue and passes it to the specified fun. See
  unwrap-obj for the format of the message."
  [queue fun]
  (let [key (:name queue)
        pkey (:processing-key queue)]
    (when-let [raw (car/wcar queue
                     (car/rpoplpush key pkey))]
      (let [result (fun (unwrap-obj queue raw))]
        (car/wcar queue
          (car/lrem pkey 1 raw))
        result))))

(defn queue-counts [{:keys [name processing-key failure-key] :as queue}]
  (let [[input processing failure] (car/wcar queue
                                             [(car/llen name)
                                              (car/llen processing-key)
                                              (car/llen failure-key)])]
    {:input input
     :processing processing
     :failure failure
     :all (+ input processing failure)}))

(defn rerun-one [{:keys [queue target-queue event-transform-fn k]
                  :or {event-transform-fn identity
                       target-queue queue}}]
  (assert queue)
  (assert (contains? #{:name :processing-key :failure-key} k))
  (let [updated-msg (-> (car/wcar queue
                                 (car/rpop (get queue k)))
                       json/decode
                       (assoc "retries" 0)
                       event-transform-fn
                       json/encode)]
    (car/wcar queue
              (car/lpush (:name target-queue) updated-msg))))

(defn rerun-failures
  "Takes all messages from the failures queue and adds them back to the
   processing queue, input queue again."
  [{:keys [queue] :as options}]
  (assert queue)
  (dotimes [_ (:failure (queue-counts queue))]
    (rerun-one (assoc options :k :failure-key))))

;; ----------------------------------------
;; worker

(defmacro with-work-body-exception-handling
  "Handles exceptions for the worker body loop.
   Why? It is abstracted so that the same exception logic can be used both for,
   within the context of a specific queue-msg being handled, and also wrapped a
   few layers up. We want this inner layer wrapped around the handling of the
   queue-msg specifically because then we can wrap it in log/with-context to get
   that info into any logged exceptions."
  [worker context & body]
  `(let [worker# ~worker]
     (try
       ~@body
       (catch Error e#
         (fatal "Fatal Error handling message" e#)
         (when (:exit-on-error @worker#)
           (System/exit 1)))
       (catch Exception e#
         (error "Error handling message" e#)
         (Thread/sleep (* 1000 (:delay @worker#)))))))

(defn worker-body [queue worker handler]
  (let [key (:name queue)
        pkey (:processing-key queue)]
    (fn []
      (while (:running @worker)
        (with-work-body-exception-handling worker queue
          (when-let [raw (car/wcar queue
                                   (car/brpoplpush key pkey (:timeout @worker)))]
            (when (seq raw)
              (let [queue-msg (unwrap-obj queue raw)]
                (with-work-body-exception-handling worker queue-msg
                  (handler queue-msg))
                (car/wcar queue
                          (car/lrem pkey 1 raw))))))))))

(defn queue-worker
  [queue handler & {:keys [delay timeout exit-on-error threads]
                    :or {delay 0
                         timeout 30
                         exit-on-error true
                         threads 1
                         }}]
  (let [worker (atom {:type "queue-worker"
                      :running true
                      :timeout timeout
                      :exit-on-error exit-on-error
                      :delay delay
                      :queue-name (:name queue)})]
    (swap! worker assoc
           :threads (mapv (fn [tnum]
                            (doto (Thread.
                                   (worker-body queue worker handler)
                                   (str "Queue worker " tnum ":" (:name queue)))
                              (.setDaemon true)
                              (.start)))
                          (range 0 threads)))
    (swap! queue-workers conj worker)
    worker))

(defn shutdown [worker]
  (swap! worker assoc :threads nil))

(defn shutdown? [worker]
  (empty? (:threads @worker)))

(defn stop-worker [qw]
  (swap! qw assoc :running false)

  ;; run in parallel because the .joins can take a long time
  (dorun (pmap (fn [t]
                 ;; convert seconds to milliseconds and double
                 (.join t (* 2000 (:timeout @qw))))
               (:threads @qw)))
  (shutdown qw)
  (swap! queue-workers #(remove (fn [w] (= w qw)) %)))

(defn- wait-until
  "Waits until (f) returns a truthy value then returns that value"
  ([f]
   (wait-until f {}))
  ([f {:keys [sleep-millis timeout]
       :or {sleep-millis 1000}
       :as options}]
   (let [start-ts (System/currentTimeMillis)]
     (loop []
       (or
        (f)
        (do
          (when timeout
            (let [now-ts (System/currentTimeMillis)]
              (when (> (- now-ts start-ts)
                       timeout)
                (throw (ex-info "Timeout exceeded" {:sleep-millis sleep-millis
                                                    :timeout timeout
                                                    :elapsed (- now-ts start-ts)
                                                    :start-ts start-ts
                                                    :now-ts now-ts})))))
          (Thread/sleep sleep-millis)
          (recur)))))))

(defn stop-workers
  "A quicker way to stop workers, rather than waiting for each in turn."
  [workers]
  (doseq [w workers]
    (future (stop-worker w)))
  (wait-until (fn []
                (every? shutdown? workers))
              {:sleep-millis 500}))


;; ----------------------------------------
;; reaper

(defn delete-active-message [queue message-text]
  (= 1 (car/wcar queue
         (car/lrem (:processing-key queue) 1 message-text))))

(defn fail-or-discard-message [queue message-text]
  (if-let [failure-key (:failure-key queue)] ; messages that will be retried
    (let [failure-count (car/wcar queue
                          (car/lpush failure-key message-text))]
      (when-let [failure-max-depth (:failure-max-depth queue)]
        (when (< failure-max-depth failure-count)
          (car/wcar queue
            (car/ltrim failure-key 0 (dec failure-max-depth))))))))

(defn inc-key [m k]
  (update m k (fnil inc 0)))

(defn reap-queue [queue retry-limit old-message-set]
  (let [active-messages (reverse (current-processing queue))]
    (doseq [message active-messages]
      (when (contains? old-message-set message)
        (let [dead-message message
              message (json/parse-string dead-message true)
              retries (get message :retries 0)]
          (if (not (delete-active-message queue dead-message)) ; race w/ message completion or other reaper
            (warn "A dead message was removed out from underneath us: " dead-message)
            (if (< retries retry-limit)
              (let [new-message (-> (inc-key message :retries)
                                    (json/generate-string))]
                (enqueue-message queue new-message))
              (fail-or-discard-message queue dead-message))))))
    (set active-messages)))

(defn reaper-body [reaper]
  (fn []
    (while (:running @reaper)
      (doseq [queue (:queues @reaper)]
        (let [queue-name (:name queue)]
          (try
            (let [retry-limit (:retries queue)
                  old-messages (get-in @reaper [:messages queue-name])
                  new-messages (reap-queue queue retry-limit old-messages)]
              (swap! reaper assoc-in [:messages queue-name] new-messages))
           (catch Exception e
             (error (str "Reaper Exception... " queue-name) e)))))
      (Thread/sleep (* 1000 (:delay @reaper))))))

(defn queue-reaper [delay & qs]
  (let [reaper (atom {:type "queue-reaper"
                      :running true
                      :delay delay
                      :queues qs
                      :messages {}})
        thread (Thread. (reaper-body reaper) (str "Queue Reaper"))]
    (swap! reaper assoc :thread thread)
    (doto thread
      (.setDaemon true)
      (.start))
    (swap! queue-reapers conj reaper)
    reaper))

(defn stop-reaper [qr]
  (swap! qr assoc :running false :thread nil)
  (swap! queue-reapers #(remove (fn [r] (= r qr)) %)))
