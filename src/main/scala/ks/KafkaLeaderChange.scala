package ks

import ks.tools.kafka.admin.KafkaAdmin

object KafkaLeaderChange {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Arguments: <zookeeper address> <kafka address> <topic:partition>")
      System.exit(-1)
    }

    changeLeader(args(0), args(1), args.drop(2).toSet)
  }

  private def changeLeader(zkAddress: String, kafkaAddress: String, tps: Set[String]): Unit = {
    println(s"Connecting to Zookeeper: $zkAddress, Kafka: $kafkaAddress")

    val ka = new KafkaAdmin(zkAddress, kafkaAddress)
    if (ka.shiftReplicas(tps)) {
      println("Reassigned partitions")
      ka.preferredReplicaElection(tps)
    }
    else println("Another reassignment in progress")
    ka.close()
  }
}
