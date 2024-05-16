package kafka.cli.examples;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class PartitionerExample implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
        if (availablePartitions.isEmpty()) {
            return 0;
        } else {
            int numberOfPartitions = availablePartitions.size();
            return BuiltInPartitioner.partitionForKey(keyBytes, numberOfPartitions);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

}
