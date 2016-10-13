(ns redismq.core-test
  (:require [clojure.test :refer :all]
            [clojure.data :refer [diff]]
            [redismq.core :refer :all]))


(defn mq-conn-spec []
  {:host (or (System/getenv "REDIS_HOST") "localhost")
   :post (or (System/getenv "REDIS_PORT") "6379")})


(defn make-test-queue [& [options]]
  (make-queue "testq" (mq-conn-spec) options))

(defmacro with-test-queue [queue & body]
  `(try
     ~@body
     (finally
       (.close (:pool ~queue)))))

(defmethod assert-expr 'deep= [msg form]
  (let [a (second form)
        b (nth form 2)]
    `(let [[only-a# only-b# _#] (diff ~a ~b)]
       (if (or only-a# only-b#)
         (let [only-msg# (str (when only-a# (str "Only in A: " only-a#))
                              (when (and only-a# only-b#) ", ")
                              (when only-b# (str "Only in B: " only-b#)))]
           (do-report {:type :fail, :message ~msg,
                          :expected '~form, :actual only-msg#}))
         (do-report {:type :pass, :message ~msg,
                        :expected '~form, :actual nil})))))

(deftest test-queues-basic
  (let [q (make-test-queue)]
    (with-test-queue q
      (let [em (flush-queue q)
            sd (current-depth q)]
        (enqueue q 1)
        (is (= (current-depth q) 1) "Queue did not grow.")
        (flush-queue q)
        (is (= (current-depth q) 0) "Queue did not flush")
        (enqueue q 1)
        (is (= [1] (map :data (current-queue q))) "Current queue not expected..")
        (is (= 1 (consume q :data)) "Queue value not expected")
        (is (= (current-depth q) 0) "Queue value not consumed")))))

(deftest test-queue-worker
  (let [q (make-test-queue)]
    (with-test-queue q
      (let [em (flush-queue q)
            results (atom [])
            sd (current-depth q)
            worker (queue-worker q (fn [m]
                                     (swap! results #(conj % (:data m))))
                                 :timeout 10)]
        (try
          (enqueue q 1)
          (enqueue q 2)
          (enqueue q 3)
          (Thread/sleep 1000)
          (is (deep= [1 2 3] @results))
          (is (= (current-depth q) 0) "Queue value not consumed")
          (enqueue q 4)
          (Thread/sleep 1000)
          (is (deep= [1 2 3 4] @results))

          (Thread/sleep 1000)
          (stop-worker worker)
          ;; we need to sleep a bit longer than the timeout
          ;; to make sure the thread dies
          (Thread/sleep 3000)
          (enqueue q 5)
          (is (= (current-depth q) 1) "Worker did not stop.")
          (finally (stop-worker worker) true))))))

(deftest test-queue-reaper
  (let [q (make-test-queue)]
    (with-test-queue q
      (let [em (flush-queue q)
            results (atom [])
            sd (current-depth q)
            reaper (queue-reaper 1 q)]
        (try
          (enqueue q 1)
          (enqueue q 2)
          (enqueue q 3)
          (Thread/sleep 1000)
          (try (consume q (fn [v] (throw (Exception. "On Purpose."))))
               (catch Exception e# nil))
          (is (= [3 2] (map :data (current-queue q))) "Results not as expected.")
          (is (= (count (current-processing q)) 1) "Processing is empty.")

          (Thread/sleep 3000)
          (testing "Processing is not empty."
            (is (deep= (count (current-processing q)) 0)))
          (is (=  (current-depth q) 3) "Message not returned to queue")
          (consume q identity)
          (consume q identity)
          (is (= (:retries (consume q identity)) 1) "Retries not iterated.")
          (finally (stop-reaper reaper) true))))))

(defn queue-info [queue]
  {:waiting (count (current-queue queue))
   :active (count (current-processing queue))
   :failed (count (failed-messages queue))})


(deftest test-reap-to-failure-queue
  (let [queue (make-test-queue {:failure-max-depth 10000
                                :retries 1})
        jobs (atom nil)
        reap (fn []
               (swap! jobs (fn [jobs] (reap-queue queue 1 jobs))))
        fail-job (fn []
                   (try
                     (consume queue (fn [_] (throw (Exception. "On Purpose."))))
                     (catch Exception e# nil)))]
    (flush-queue queue)
    (is (deep= {:waiting 0 :active 0 :failed 0}
               (queue-info queue))
        "empty state")
    (enqueue queue 1)
    (is (deep= {:waiting 1 :active 0 :failed 0}
               (queue-info queue))
        "1 job waiting")
    (fail-job)
    (is (deep= {:waiting 0 :active 1 :failed 0}
               (queue-info queue))
        "1 job marked processing (but it's really dead)")
    (reap)
    (is (= 1 (count @jobs))
        "1 job to be detected upon next reap")
    (is (deep= {:waiting 0 :active 1 :failed 0}
               (queue-info queue))
        "Nothing should have happened")
    (reap)
    (is (deep= {:waiting 1 :active 0 :failed 0}
               (queue-info queue))
        "Job should be reaped back to wait queue")
    (fail-job)
    (is (deep= {:waiting 0 :active 1 :failed 0}
               (queue-info queue))
        "Job should be processing")
    (reap)
    (is (deep= {:waiting 0 :active 1 :failed 0}
               (queue-info queue))
        "Job should be processing")
    (reap)
    (is (deep= {:waiting 0 :active 0 :failed 1}
               (queue-info queue))
        "Job should be dead")))

(deftest test-reap-to-failure-queue--sizelimit
  (let [queue (make-test-queue {:failure-max-depth 2})
        jobs (atom nil)
        reap (fn []
               (swap! jobs (fn [jobs] (reap-queue queue 0 jobs))))
        fail-job (fn []
                   (try
                     (consume queue (fn [_] (throw (Exception. "On Purpose."))))
                     (catch Exception e# nil)))]
    (flush-queue queue)
    (enqueue queue 1)
    (enqueue queue 2)
    (enqueue queue 3)
    (fail-job)
    (fail-job)
    (fail-job)
    (reap)
    (reap)
    (is (deep= {:waiting 0 :active 0 :failed 2}
               (queue-info queue))
        "One message should have been discarded")
    (is (= #{2 3}
           (set (map :data (failed-messages queue))))
        "Most recent failures are kept")))

(deftest test-queue--sizelimit
  (let [queue (make-test-queue {:max-depth 5})
        jobs (atom nil)
        reap (fn []
               (swap! jobs (fn [jobs] (reap-queue queue 1 jobs))))
        fail-job (fn []
                   (try
                     (consume queue (fn [_] (throw (Exception. "On Purpose."))))
                     (catch Exception e# nil)))]
    (flush-queue queue)
    (doseq [x (range 0 6)]
      (enqueue queue x))
    (is (deep= {:waiting 5 :active 0 :failed 0}
               (queue-info queue))
        "Should only see 5 waiting")))
