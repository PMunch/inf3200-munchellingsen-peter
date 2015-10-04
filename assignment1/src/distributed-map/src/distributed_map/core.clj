(ns distributed-map.core
  (:gen-class)
  (:import [java.net ServerSocket Socket SocketException]
        [java.io InputStreamReader OutputStreamWriter IOException PrintWriter]
        [clojure.lang LineNumberingPushbackReader]
        [clojure.string])
  (:use [clojure.string :only [split]]
        [clojure.java.shell :only [sh]])
  (:require [clojure.java.io :as io]))

(defn receive-msg
  "Read a line of textual data from the given socket"
  [socket]
  (.readLine (io/reader socket)))

(defn send-msg
  "Send the given string message out over the given socket"
  [socket msg]
  (let [writer (io/writer socket)]
      (.write writer msg)
      (.flush writer)))

(defn send-value
	"Sends a key value pair to a server"
	[host port k value]
	(with-open [sock (Socket. host port)]
		(send-msg sock (str "PUT " k ":" value \newline))
		(receive-msg sock)))

(defn get-value
	"Requests a value with key k from server"
	[server port k]
	(with-open [sock (Socket. server port)]
		(send-msg sock (str "GET " k \newline))
		(receive-msg sock)))

(defn get-info
	"Requests how many values are stored on a given server"
	[server port]
	(with-open [sock (Socket. server port)]
		(send-msg sock (str "INFO" \newline))
		(receive-msg sock)))

(defn on-thread [f]
	(.start (Thread. f)))

(defn create-hash
	"Creates a hash of in which is a sum of the input mod size"
	[in size]
	;(mod (reduce + (map int (seq in))) size)
	
	(mod (loop [i 0
				h 0]
				(if (> (count in) i)
					(recur	(+ i 1)
						(mod (+ (* h 31) (int (.charAt in i))) Integer/MAX_VALUE))
				h)) size)
	
	;(mod (hash in) size)
	)

;(rand-nth hosts)
(defn interactive-shell
	"Sets up an interactive shell to directly interact with the node network"
	[hosts]
	(def running true)
	(while (= running true)
		(print "dist-keymap => ")
		(flush)
		(let 	[phrase (read-line)
				 tokens (split phrase #" ")
				 unrecognized "Unrecognized command. Only 'GET <key>', 'PUT <key>:<value>', 'INFO', '.help' and '.quit' supported."]
			(case (get tokens 0)
			"GET"	(when (= (count tokens) 2)
	        			(println (get-value (rand-nth hosts) 1234 (get (split phrase #" ") 1))))
        	"PUT"	(when (= (count tokens) 2)
		        		(let [pair (split (get tokens 1) #":")]
		        			(when (= (count pair) 2))
		        				(println (send-value (rand-nth hosts) 1234 (get pair 0) (get pair 1)))))
        	"INFO"	(doseq [host hosts]
        				(println (str host " has " (get-info host 1234) " records")))
        	".help" (println "To send a value to the distributed hash map use 'PUT <key>:<value>' and to retrieve a value use 'GET <key>'. To get information about how many pairs are stored on each node run 'INFO'. '.help' displays this message and '.quit' shuts down all nodes and exits the shell.")
        	".quit"	(def running false)
        	(println unrecognized))))
	(println "Shutting down shell"))

(defn create-server
	"Sets up an entry point server to redirect commands to the node network"
	[port hosts]
	(with-open [server-sock (ServerSocket. port)]
		(while true
			(let [incoming (.accept server-sock)]
			    (on-thread #(with-open [request-sock incoming]
					(let [msg-in (receive-msg request-sock)]
						(with-open [node-sock (Socket. (rand-nth hosts) port)]
							(println (str "Passing on message " msg-in))
							(send-msg node-sock (str msg-in \newline))
							(send-msg request-sock (str "" (receive-msg node-sock) \newline))))))))))

(defn create-node
	"Sets up a node in the distributed map"
	[port hosts]
	(def values (atom {}))
	(let [host (get (split (.getCanonicalHostName (java.net.InetAddress/getLocalHost)) #"\.") 0)]
		(println (str "Opening node on " host))
		(with-open [server-sock (ServerSocket. port)]
			(while true
				(let [incoming (.accept server-sock)]
				    (on-thread #(with-open [sock incoming]
				    	(let 	[msg-in (receive-msg sock)
				    		 tokens (split msg-in #" ")]
				    	(if (= (count tokens) 2)
					    	(case (get tokens 0)
					    		"GET" (let [mapkey (get tokens 1)
					    					 truehost (nth hosts (create-hash mapkey (count hosts)))]
					    				(println (str \' msg-in \' " request bound for " (if (= truehost host) "this node." (str truehost ", forwarding request.") )))
					    				(if (= truehost host)
					    				(let [val-snap @values] (send-msg sock (or (val-snap (keyword mapkey)) "nil"))
					    					(println (str "Returning " (or (val-snap (keyword mapkey)) "nil"))))
					    				(send-msg sock (get-value truehost 1234 mapkey))))
					    		"PUT" (let [keyval (split (get tokens 1) #":")]
					    					(if (and (= (count keyval) 2) (not= (get keyval 0) ""))
					    					(let [truehost (nth hosts (create-hash (get keyval 0) (count hosts)))]
						    					(println (str \' msg-in \' " request bound for " (if (= truehost host) "this node." (str truehost ", forwarding request.") )))
						    					(if (= truehost host)
						    					(do (swap! values assoc (keyword (get keyval 0)) (get keyval 1))(send-msg sock "OK"))
						    					(do (send-msg sock (send-value truehost 1234 (get keyval 0) (get keyval 1))))))
					    					(do (send-msg sock "ERROR")(println (str "Bad query: " \' msg-in \' " invalid key:value combination.")))))
					    		(do (send-msg sock "ERROR")(println (str "Received unknown op-code: " msg-in))))
							(if (= (get tokens 0) "INFO")
								(let [val-snap @values] (println (format "'INFO' request answered with: %d" (count val-snap)))
									(send-msg sock (format "%d" (count val-snap))))
								(do (send-msg sock "ERROR")(println (str "Bad query: " \' msg-in \' " unknown command or bad number of arguments")))))))))))))

(defn -main
  "Handle arguments and start program based on chosen mode"
  [& args]
  (def usagenotice "To run this program a role has to be specified. The current available roles are 'node', 'killnodes', 'server', 'shell', and 'shellserver'. All modes also requires a list of hosts in a newline separated file named 'hostfile'.\n\tnode - start a node in the distributed network\n\tkillnodes - kill all nodes in the hostfile\n\tserver - start a server which randomly passes on commands to the nodes\n\tshell - interactive shell to pass commands to random hosts in an existing network\n\tshellserver - start a new network and opens an interactive shell. This mode will also turn off nodes on quit.")
  (def hosts (with-open [rdr (clojure.java.io/reader (.getCanonicalPath (java.io.File. "./hostfile")))]
    (reduce conj [] (line-seq rdr))))
  (let [mode (nth args 0)]
  (def running-nodes (atom false))
  ;Manage nodes
  (when (or (= mode "shellserver") (= mode "server"))
  	(reset! running-nodes true)
  	;If nodes were not shut down before close, print reminder to turn them off manually
  	(.addShutdownHook (Runtime/getRuntime) (Thread. #(when (= @running-nodes true)(println "Server was unable to shut down nodes so they must be shut down manually!"))))
  	(doseq [host hosts]
		(println (str "Trying to start node on " host))
		(let [here (.getCanonicalPath (java.io.File. "."))]
			(println (sh "nohup" "ssh" host (str "bash -c 'cd " here " && java -jar " here "/distmap.jar node' >" here "/log-" host ".log 2>&1 &"))))))
  (case mode
  "node"	(create-node 1234 hosts)
  "shellserver"	(interactive-shell hosts)
  "shell"	(interactive-shell hosts)
  "server"	(create-server 1234 hosts)
  "killnodes" (do (reset! running-nodes false)
				(doseq [host hosts]
				(println (str "Trying to bring down node on " host))
				(println (sh "ssh" host "pkill" "-f 'distmap.jar'"))))
  	(println usagenotice))
  ;Ensure that nodes that have been started are shut down
  (when (or (= mode "server")(= mode "shellserver"))
	  (doseq [host hosts]
			(println (str "Trying to bring down node on " host))
			(println (sh "ssh" host "pkill" "-f 'distmap.jar'")))
			(reset! running-nodes false)))
	;Shutdown other threads such as shell commands
	(shutdown-agents))
