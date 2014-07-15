gostat
======

A statistics collector / aggregator, written in go

## Installation ##

### Cassandra ###

1. Install the DataStax Community Edition of Apache Cassandra 2.0 via [these instructions](http://www.datastax.com/documentation/cassandra/2.0/cassandra/install/installTarball_t.html)

2. Start Cassandra

<pre><code>
sudo nohup $HOME/cassandra-2.0.8/bin/cassandra

</code></pre>

### Environment Variables ###

Export the following environment variables

<pre><code>
export GOPATH=$HOME/gostat
export GOBIN=$GOPATH/bin
export PATH=$PATH:$GOBIN

</code></pre>

### Dependencies ###

Install the following package dependencies

<pre><code>
go get github.com/onsi/ginkgo/ginkgo
go get github.com/onsi/gomega
go get github.com/gocql/gocql

</code></pre>