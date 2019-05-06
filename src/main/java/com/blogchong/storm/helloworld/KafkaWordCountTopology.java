package com.blogchong.storm.helloworld;

import java.util.Arrays;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.blogchong.storm.helloworld.bolt.PrintBolt;
import com.blogchong.storm.helloworld.bolt.WordCountBolt;
import com.blogchong.storm.helloworld.bolt.WordNormalizerBolt;
import com.blogchong.storm.helloworld.spout.RandomSentenceSpout;

/**
 * @Author: blogchong
 * @Blog: www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @Mailbox: blogchong@163.com
 * @QQGroup: 191321336
 * @Weixin: blogchong
 * @Data: 2015/4/7
 * @Describe: 启动主类，拓扑构建
 */

public class KafkaWordCountTopology {

    private static TopologyBuilder builder = new TopologyBuilder();

    public static void main(String[] args) {
    	
    	 String zks ="localhost:2181";
         String topic ="test_kafka";
         String zkRoot ="/consumers"; // default zookeeper root configuration for storm
         String id = "word";

         BrokerHosts brokerHosts = new ZkHosts(zks);
         SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
         spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//         spoutConf.startOffsetTime = -1;
         spoutConf.forceFromStart = false;
         spoutConf.zkServers = Arrays.asList(new String[]{"localhost"});
         spoutConf.zkPort = 2181;

         //set hdfs bolt
         SyncPolicy syncPolicy = new CountSyncPolicy(1000);
         
         FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.MINUTES);
         
         FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/storm/").withPrefix("app_").withExtension(".log");

         HdfsBolt hdfsBolt = new HdfsBolt()
        		 .withFsUrl("hdfs://localhost:9000")
        		 .withFileNameFormat(fileNameFormat)
        		 .withRotationPolicy(rotationPolicy)
        		 .withSyncPolicy(syncPolicy);

        Config config = new Config();

        builder.setSpout("RandomSentence", new KafkaSpout(spoutConf), 1);
        
        builder.setBolt("WordNormalizer", new WordNormalizerBolt(), 1).shuffleGrouping(
                "RandomSentence");
        builder.setBolt("WordCount", new WordCountBolt(), 2).fieldsGrouping("WordNormalizer",
                new Fields("word"));
        builder.setBolt("Print", new PrintBolt(), 1).shuffleGrouping(
                "WordCount");
        builder.setBolt("hdfs-bolt", hdfsBolt, 1).fieldsGrouping("Print",
        		new Fields("word"));


        config.setDebug(false);

        //通过是否有参数来控制是否启动集群，或者本地模式执行
        if (args != null && args.length > 0) {
            try {
                config.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0], config,
                        builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", config, builder.createTopology());
        }
    }
}

