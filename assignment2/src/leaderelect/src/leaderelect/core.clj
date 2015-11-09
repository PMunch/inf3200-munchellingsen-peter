(ns leaderelect.core
  (:gen-class)
  (:import [sun.misc Signal SignalHandler]
  		[java.net ServerSocket Socket SocketException]
        [java.io InputStreamReader OutputStreamWriter IOException PrintWriter FileNotFoundException]
        [clojure.lang LineNumberingPushbackReader]
        [clojure.string])
  (:use [clojure.string :only [split join]]
        [clojure.java.shell :only [sh]])
  (:require [clojure.java.io :as io]))

(defn create-hash
	"Creates a hash of in which fits in size with minimal rehashing given various sizes. Optional n argument can be specified if it has been precomputed"
	[in size]
	(let [n (Math/ceil (/ (Math/log size) (Math/log 2)))
		  mask (int (- (Math/pow 2 n) 1))]
		(loop [in in
			   hitN false]
			(let [Hv (loop [i 0
							h 0]
						(if (> (count in) i)
							(recur	(+ i 1)
								(mod (+ (* h 31) (int (.charAt in i))) Integer/MAX_VALUE))
						h))
		  		  first-n (bit-and Hv mask)]
				(if (<= first-n size)
					(if (< first-n size)
						[first-n hitN]
						(recur (str Hv) true))
					(recur (str Hv) hitN))))))

(defn mod-hash
	"Creates a hash of in fitting in size buckets."
	[in size]
	(mod (hash in) size))

(defn parse-http
	"Parse a HTTP request"
	[request]
	(def parsed-request {})
	(let [header-body (split request #"\n\n" 2)
		  [header body] header-body
		  header-lines (split header #"\n")
		  request-line (split (get header-lines 0) #" ")
		  [method destination protocol] request-line]
		  	(def parsed-request (assoc parsed-request :method method :destination destination :protocol protocol :body body))
		  	(def parsed-request  (loop [i 1
		  		   parsed-request parsed-request]
		  		(if (< i (count header-lines))
		  		(let [header-field (get header-lines i)
		  			  [keyw value] (split header-field #":\s*")]
		  		(recur 	(inc i)
		  				(assoc parsed-request (keyword keyw) value)))
		  		parsed-request))))
	parsed-request)

(defn receive-msg
  "Read a line of textual data from the given socket"
  [socket]
  (.readLine (io/reader socket)))

(defn receive-http
	"Receive a proper HTTP request"
	[socket]
	(let [reader (io/reader socket)
		  firstline (or (.readLine reader) "")
		  method (get (split firstline #" ") 0)]
		(loop [message firstline
			   readnewline (or (= method "PUT") (= method "POST"))]
			   (let [line (.readLine reader)]
			   		(if (not (nil? line))
			   			(if (or (not= "" line) readnewline)
				   			(recur (str message "\n" line) (= true (and readnewline (not= "" line))))
				   			message)
			   			nil)))))

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

(defn get-leader
	"Requests who the leader of the network is"
	[server port]
	(with-open [sock (Socket. server port)]
		(send-msg sock (str "LEADER" \newline))
		(receive-msg sock)))

(defn get-nodes
	"Requests who the leader of the network is"
	[server port]
	(with-open [sock (Socket. server port)]
		(send-msg sock (str "NODES" \newline))
		(receive-msg sock)))

(defn join-request
	"Requests to join the network at a host"
	[server port after]
	(with-open [sock (Socket. server port)]
		(send-msg sock (str "JOIN " after \newline))
		(receive-msg sock)))

(defn leave-request
	"Requests to join the network at a host"
	[server port after]
	(with-open [sock (Socket. server port)]
		(send-msg sock (str "LEAVE " after \newline))
		(receive-msg sock)))

(defn name-to-ip
	"Returns a name to an ip and appends text"
	[server]
	(.getHostAddress (java.net.InetAddress/getByName server))
	)

(defn on-thread [f]
	(.start (Thread. f)))

(defn interactive-shell
	"Sets up an interactive shell to directly interact with the node network"
	[hosts port]
	(def running true)
	(while (= running true)
		(print "dist-keymap => ")
		(flush)
		(let 	[phrase (read-line)
				 tokens (split phrase #" ")
				 unrecognized "Unrecognized command. Only 'GET <key>', 'PUT <key>:<value>', 'INFO', '.help' and '.quit' supported."]
			(case (get tokens 0)
			"GET"	(when (= (count tokens) 2)
	        			(println (get-value (rand-nth hosts) port (get (split phrase #" ") 1))))
        	"PUT"	(when (= (count tokens) 2)
		        		(let [pair (split (get tokens 1) #":")]
		        			(when (= (count pair) 2))
		        				(println (send-value (rand-nth hosts) port (get pair 0) (get pair 1)))))
        	"INFO"	(doseq [host hosts]
        				(println (str host " has " (get-info host port) " records")))
        	"LEADER" (let [rand-host (rand-nth hosts)]
        				(println (str rand-host " sees " (get-leader rand-host port) " as leader")))
        	"NODES"	(let [rand-host (rand-nth hosts)]
        				(println (str rand-host " is connected to:\n" (get-nodes rand-host port))))
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
			    	(while (not (.isClosed request-sock))
						(let [msg-in (receive-http request-sock)]
							(if (not= msg-in nil)
								(let [request (parse-http msg-in)
								  method (:method request)
								  destination (:destination request)
								  truedest (last (re-seq #"[a-z\.]+" destination))]
								(println request)
								(def message (case method
								"GET"	(case destination 
									"/getCurrentLeader" "LEADER"
									"/getNodes" "NODES"
									(str "GET " truedest))
								"PUT"	(str "PUT " truedest ":" (:body request))
								nil))
								(if (not= message nil)
									(do (with-open [node-sock (Socket. (rand-nth hosts) port)]
										(send-msg node-sock (str message \newline))
										(let [response (str "" (receive-msg node-sock))]
											(send-msg request-sock (str (case response
												"400" "HTTP/1.1 400 Bad Request"
												"404" "HTTP/1.1 404 Not Found"
												"501" "HTTP/1.1 501 Not Implemented"
												"201" "HTTP/1.1 201 Created"
												"200" "HTTP/1.1 200 OK"
												(str "HTTP/1.1 200 OK\r\n\r\n" (case destination
													"/getNodes" (join "\n" (map str (map name-to-ip (split response #", "))(repeat (str ":" port))))
													"/getCurrentLeader" (str (name-to-ip response) ":" port)
													response) "\r\n\r\n")))))))
									(send-msg request-sock "HTTP/1.1 501 Not Implemented"))
								(.close request-sock)))))))))))

(defn create-node
	"Sets up a node in the distributed map"
	[port initial-hosts & initial-values]
	(def values (atom (or initial-values {})))
	(println @values)
	(def current-hosts (atom initial-hosts))
	(let [host (get (split (.getCanonicalHostName (java.net.InetAddress/getLocalHost)) #"\.") 0)]
		(println (str "Opening node on " host))
		(if (some #{host} @current-hosts)
			(do
			(let [handler (reify SignalHandler (handle [this sig]
			  		(println "Leaving")
			  		(swap! current-hosts #(remove #{host} %))
			  		(loop [hosts @current-hosts
			  			  prefer-after (last hosts)
						  initial-request (leave-request (first hosts) port prefer-after)]
						  (let [request-tokens (split initial-request #" ")]
					  (if (= "200" (first request-tokens))
					  	(do (println "Success!")
					  		(loop [kv (loop [kvpairs (rest request-tokens)
											 hosts-left (next hosts)]
						  			(if (not= hosts-left nil)
							  			(let [tokens (split (leave-request (first hosts-left) port prefer-after) #" ")]
							  				(if (= "200" (first tokens))
							  					(recur (concat kvpairs (rest tokens)) (next hosts-left))
							  					(recur kvpairs hosts-left)))
							  			kvpairs))]
					  			(when (not= (first kv) nil)
						  			(let [truehost (nth hosts (first (create-hash (first kv) (count hosts))))
						  				  keyvalue (split (first kv) #":")]
						  				(send-value truehost port (first keyvalue) (second keyvalue))
						  				(recur (rest kv))))))
					  	(do (println "Failure!")
					  		(println request-tokens)
					  		(recur hosts prefer-after (leave-request (first hosts) port prefer-after))))))
					(shutdown-agents)
					(System/exit 0)))]
			  	(Signal/handle (Signal. "INT") handler)
			  	(Signal/handle (Signal. "TERM") handler))
			(with-open [server-sock (ServerSocket. port)]
				(while true
					(let [incoming (.accept server-sock)]
					    ;(on-thread #
					    	(with-open [sock incoming]
					    	(let 	[msg-in (receive-msg sock)
					    		 tokens (split msg-in #" ")
					    		 hosts @current-hosts]
					    	(if (= (count tokens) 2)
						    	(case (get tokens 0)
						    		"GET" (let [mapkey (get tokens 1)
						    					 truehost (nth hosts (first (create-hash mapkey (count hosts))))]
						    				(println (str \' msg-in \' " request bound for " (if (= truehost host) "this node." (str truehost ", forwarding request.") )))
						    				(if (= truehost host)
						    				(let [val-snap @values] (send-msg sock (or (val-snap (keyword mapkey)) "404"))
						    					(println (str "Returning " (or (val-snap (keyword mapkey)) "nil"))))
						    				(send-msg sock (get-value truehost port mapkey))))
						    		"PUT" (let [keyval (split (get tokens 1) #":")]
						    					(if (and (= (count keyval) 2) (not= (get keyval 0) ""))
						    					(let [truehost (nth hosts (first (create-hash (get keyval 0) (count hosts))))]
							    					(println (str \' msg-in \' " request bound for " (if (= truehost host) "this node." (str truehost ", forwarding request.") )))
							    					(if (= truehost host)
							    					(let [keyw (keyword (get keyval 0))
							    						  code (case (keyw @values)
							    						  	nil "201"
							    						  	"200")]
							    						(swap! values assoc keyw (get keyval 1))
							    						(send-msg sock code))
							    					(send-msg sock (send-value truehost port (get keyval 0) (get keyval 1)))))
						    					(do (send-msg sock "400")(println (str "Bad query: " \' msg-in \' " invalid key:value combination.")))))
						    		"JOIN" (let [after (get tokens 1)]
						    				(if (= after (last hosts))
						    					(let [val-snap @values
						    						  to-add (get (split (.getCanonicalHostName (.getInetAddress sock)) #"\.") 0)]
						    						(swap! current-hosts conj to-add)
						    						(let [hosts @current-hosts]
						    							(println val-snap)
						    							(loop [response "200 "
						    								   pair (first val-snap)
						    								   other (next val-snap)]
						    								   (println (str "Got " response " doing " pair))
						    								   (if (not= nil pair)
						    								   		(let [truehost (nth hosts (first (create-hash (name (first pair)) (count hosts))))]
						    								   		(if (= to-add truehost)
						    								   			(do (swap! values dissoc (first pair))
						    								   				(recur (str response (name (first pair)) ":" (second pair) " ") (first other) (next other)))
						    								   			(do (if (not= truehost host)
						    								   				(do (send-value truehost port (name (first pair)) (second pair))
						    								   					(recur response (first other) (next other)))
						    								   				(recur response (first other) (next other))))))
						    								   		(send-msg sock response)))))
						    					(send-msg sock (str "409 " (join " " hosts)))))
									"LEAVE" (let [after (get tokens 1)]
											(println (str "Received LEAVE request " after " " (last hosts)))
											(if (or (= after (last hosts)) (and (= (last (butlast hosts)) after) (= (last hosts) (get (split (.getCanonicalHostName (.getInetAddress sock)) #"\.") 0))))
												(do (println "Should move last to nodes position and rehash")
			  										(loop [hosts @current-hosts]
			  											(if (= after (last hosts))
			  												(when (not (compare-and-set! current-hosts hosts (pop (assoc hosts (.indexOf hosts (get (split (.getCanonicalHostName (.getInetAddress sock)) #"\.") 0)) after))))
			  													(recur @current-hosts))
			  												(when (not (compare-and-set! current-hosts hosts (pop hosts)))
			  													(recur @current-hosts))))
			  										(let [hosts @current-hosts
			  											  val-snap @values]
								    					(send-msg sock (str "200 " (loop [kvpair (first val-snap)
								    						   kvrest (next val-snap)
								    						   kvstring ""]
								    						(if (not= kvpair nil)
								    							(let [k (name (first kvpair))
									    							v (second kvpair)
									    							newhost (nth hosts (first (create-hash k (count hosts))))]
										    						(when (not= newhost (if (= after host) nil host))
										    							(swap! values dissoc (first kvpair))
										    							(recur (first kvrest) (next kvrest) (str kvstring k ":" v " "))))
								    							kvstring))))))
												(send-msg sock (str "409 " (join " " hosts)))))
						    		(do (send-msg sock "501")(println (str "Received unknown op-code: " msg-in))))
								(case (get tokens 0)
									"INFO"	(let [val-snap @values] (println (format "'INFO' request answered with: %d" (count val-snap)))
												(send-msg sock (format "%d" (count val-snap))))
									"LEADER" (send-msg sock (first hosts))
									"NODES" (send-msg sock (join ", " hosts))
									(do (send-msg sock "400")(println (str "Bad query: " \' msg-in \' " unknown command or bad number of arguments")))))))))));)
			(do (println "Not in network!")
				(let [headlist (split (get-nodes (rand-nth initial-hosts) port) #", ")
					  prefer-after (last initial-hosts)
					  initial-request (join-request (first headlist) port prefer-after)]
					  (println (str "Doing request " (first headlist) " " port " " prefer-after))
					  (println initial-request)
					  (let [request-tokens (split initial-request #" ")]
					  (if (= "200" (first request-tokens))
					  	(do (println "Success!")
					  		(swap! current-hosts conj host)
					  		(loop [kvpairs (map #(split % #":") (rest request-tokens))
					  			   hosts (next headlist)]
					  			(println (str "Received pairs: " (first kvpairs)))
					  			(when (not= (first kvpairs) nil)
						  			(loop [pair (first kvpairs)
						  				   r (next kvpairs)]
						  				(let [k (first pair)
						  				   	v (second pair)]
						  				(swap! values assoc (keyword k) v)
						  				(when (not= (first r) nil)
						  					(recur (first r) (next r))))))
					  			(when (not= hosts nil)
						  			(recur (loop [resp-tokens (split (join-request (first hosts) port prefer-after) #" ")]
						  				(if (not= "200" (first resp-tokens))
						  					(recur (split (join-request (first hosts) port prefer-after) #" "))
						  					(map #(split % #":") (rest resp-tokens)))) (next hosts))))
					  		(println @values)
					  		(recur port @current-hosts @values))
					  	(do (println "Failure!")
					  		(println (rest request-tokens))
					  		(recur port (rest request-tokens) @values)))))))))

(defn -main
  "Handle arguments and start program based on chosen mode"
  [& args]
  (def usagenotice "To run this program a role has to be specified. The current available roles are 'node', 'killnodes', 'server', 'shell', and 'shellserver'. All modes also requires a list of hosts in a newline separated file named 'hostfile'.\n\tnode - start a node in the distributed network\n\tkillnodes - kill all nodes in the hostfile\n\tserver - start a server which randomly passes on commands to the nodes\n\tshell - interactive shell to pass commands to random hosts in an existing network\n\tshellserver - start a new network and opens an interactive shell. This mode will also turn off nodes on quit.")
  (try(def hosts (with-open [rdr (clojure.java.io/reader (.getCanonicalPath (java.io.File. "./hostfile")))]
    (reduce conj [] (line-seq rdr))))
  (catch FileNotFoundException e (def hosts [])))
  (let [mode (nth args 0)
  	    port (Integer. (nth args 1 1234))]
	  (def running-nodes (atom false))
	  ;Manage nodes
	  (when (or (= mode "shellserver") (= mode "server"))
	  	(reset! running-nodes true)
	  	(when (or (= mode "server")(= mode "shellserver"))(let [handler (reify SignalHandler (handle [this sig]
		  (doseq [host hosts]
				(println (str "Trying to bring down node on " host))
				(println (sh "ssh" host "pkill" "-9 -f 'distmap.jar'")))
				(shutdown-agents)
				(System/exit 0)))]
	  	(Signal/handle (Signal. "INT") handler)
	  	(Signal/handle (Signal. "TERM") handler)))
	  	(doseq [host hosts]
			(println (str "Trying to start node on " host))
			(let [here (.getCanonicalPath (java.io.File. "."))]
				(println (sh "nohup" "ssh" host (str "bash -c 'cd " here " && java -jar " here "/distmap.jar node " port "' >" here "/log-" host ".log 2>&1 &"))))))
	  (case mode
	  "node"	(create-node port hosts)
	  "node-join" (create-node port (split (get-nodes (rand-nth hosts) port) #", "))
	  "shellserver"	(interactive-shell hosts port)
	  "shell"	(interactive-shell hosts port)
	  "server"	(create-server port hosts)
	  "killnodes" (do (reset! running-nodes false)
					(doseq [host hosts]
					(println (str "Trying to bring down node on " host))
					(println (sh "ssh" host "pkill" "-9 -f 'distmap.jar'"))))
	  "test"	(println (parse-http "GET /test HTTP/1.1\nUser-Agent: Mozilla/4.0 (compatible; MSIE5.01; Windows NT)\nHost: www.tutorialspoint.com\nAccept-Language: en-us\nAccept-Encoding: gzip, deflate\nConnection: Keep-Alive\n\nTest"))
	  	(println usagenotice))
	  ;Ensure that nodes that have been started are shut down
	  (when (or (= mode "server")(= mode "shellserver"))
		  (doseq [host hosts]
				(println (str "Trying to bring down node on " host))
				(println (sh "ssh" host "pkill" "-9 -f 'distmap.jar'")))
				(reset! running-nodes false)))
	;Shutdown other threads such as shell commands
	(shutdown-agents))
