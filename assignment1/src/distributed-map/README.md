# Distributed map implementation

This program runs a distributed map on a set of hosts

## Compilation

To compile you will need the Leiningen Clojure system. For conveniance a precompiled `.jar` file can be found in `target/uberjar`.

## Usage
Before running the program a hostfile must be supplied along with the `.jar` (same folder, named `hostfile`) this hostfile can be generated by the `generate_hosts.sh` shell script. To run this script simply use:

    $ sh generate_hosts.sh [number]

Where `number` is the number of nodes wanted. This must be run on the cluster.

To run the main program use:

    $ java -jar distributed-map-0.1.0-standalone.jar [mode]

Where the mode is one of the following:
* __node__ - start a node in the distributed network
* __killnodes__ - kill all nodes in the hostfile
* __server__ - start a server which randomly passes on commands to the nodes
* __shell__ - interactive shell to pass commands to random hosts in an existing network
* __shellserver__ - start a new network and opens an interactive shell. This mode will also turn off nodes on quit.

## License

Distributed under the Eclipse Public License