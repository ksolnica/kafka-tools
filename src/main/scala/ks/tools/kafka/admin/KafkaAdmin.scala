package ks.tools.kafka.admin

import java.util.Properties

import kafka.admin.ReassignPartitionsCommand.Throttle
import kafka.admin.{PreferredReplicaLeaderElectionCommand, ReassignPartitionsCommand}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{TopicPartition, TopicPartitionReplica}

class KafkaAdmin(zkAddress: String, kafkaAddress: String) {

  val zk = KafkaZkClient.apply(zkAddress, isSecure = false, 30000, 30000, 1, Time.SYSTEM)

  def close(): Unit = {
    zk.close()
  }

  def shiftReplicas(tps: Set[String]): Boolean = {
    val partitions = tps.map(topicPartition)

    val assignment = zk.getReplicaAssignmentForTopics(partitions.map(tp => tp.topic()))
    println(s"Current topic partitions: $assignment")

    val replicas = partitions.map(p => (p, shift(assignment.get(p).orNull)))
    println(s"Will reassign partitions: $replicas")
    reassign(replicas.toMap)
  }

  def preferredReplicaElection(tps: Set[String]): Unit = {
    val partitions = tps.map(topicPartition)
    val command = new PreferredReplicaLeaderElectionCommand(zk, partitions)
    command.moveLeaderToPreferredReplica()
  }

  private def reassign(partitions: Map[TopicPartition, Seq[Int]]): Boolean = {
    if (zk.reassignPartitionsInProgress()) false
    else {
      val rs = Map.empty[TopicPartitionReplica, String]
      val opts = kafkaOpts()
      val client = new AdminZkClient(zk)
      val command = new ReassignPartitionsCommand(zk, Some(opts), partitions, rs, client)
      command.reassignPartitions(Throttle(-1, -1), 10000)
    }
  }

  private def kafkaOpts(): AdminClient = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "ks-tools-kafka")
    AdminClient.create(props)
  }

  private def topicPartition(tp: String): TopicPartition = {
    val arr = tp.split(":")
    if (arr.length < 2) null
    else new TopicPartition(arr(0), arr(1).toInt)
  }

  private def shift(sequence: Seq[Int]): Seq[Int] = {
    val shift: Seq[Int] => Seq[Int] = a => (a drop 1) :+ a.head
    shift(sequence)
  }
}
