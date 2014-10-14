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
export GOBIN=$GOPATH/bin
export PATH=$PATH:$GOBIN

</code></pre>

### Dependencies ###

Install Nanomsg (OS X)

<pre><code>
brew install nanomsg

</code></pre>

Install Protocol Buffers (OS X)

<pre><code>
brew install protobuf
</code></pre>


Install the following package dependencies

<pre><code>
go get github.com/onsi/ginkgo/ginkgo
go get github.com/onsi/gomega

</code></pre>

If you are using <code>godep</godep>, then just <code>godep restore</code>, otherwise, install the rest of these dependencies:

<pre><code>
go get github.com/gocql/gocql
go get github.com/op/go-nanomsg
go get github.com/cihub/seelog
</code></pre>

Rebuild gogoprotobuffer messages

<pre><code>
go get -u code.google.com/p/gogoprotobuf/{proto,protoc-gen-gogo,gogoproto}
protoc --gogo_out=. -I=.:code.google.com/p/gogoprotobuf/protobuf -I=$GOPATH/src/ -I=$GOPATH/src/code.google.com/p/gogoprotobuf/protobuf *.proto
</code></pre>

### Create the Cassandra Data Store ###

from the project root
```
cd cassandra
./cassandra.sh
```

