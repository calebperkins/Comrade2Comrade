# Comrade to Comrade

A MapReduce framework using Bamboo DHT.

# Authors

* Caleb Perkins
* Alex Slover

# Build and test

Run `ant compile` followed by `ant test`.

# Running

`ant client -Dhost=localhost:PORT -Djob=demos.WordCount -Din=words.json -Dout=/tmp/results.json`

To run a node without job, `ant server -Dhost=locahost:PORT`

Start at least one node with hostname `localhost:3200` to serve as the gateway.
