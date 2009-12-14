{
 :default
 {
  :hbase.client.retries.number 5
  :ipc.client.connect.max.retries 3
  :hbase.master "localhost:60000"
  :hbase.zookeeper.quorum "localhost"
  }
 :test
 {
  :hbase.master "localhost:60000"
  :hbase.zookeeper.quorum "localhost"
  }
 :sample-distributed
  {
  :hbase.client.retries.number 5
  :ipc.client.connect.max.retries 3
  :hbase.master "remote-server:60000"
  :hbase.zookeeper.quorum "remote-server"
  :hbase.cluster.distributed true
  :hbase.rootdir "hdfs://remote-server:50001/hbase"
  }
 }