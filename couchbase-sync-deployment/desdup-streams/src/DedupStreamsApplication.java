2025-10-23 22:07:20.788  INFO 13140 --- [           main] o.a.k.s.p.i.a.AssignorConfiguration      : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer] Cooperative rebalancing enabled now
2025-10-23 22:07:20.797  INFO 13140 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 2.8.1
2025-10-23 22:07:20.797  INFO 13140 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: 839b886f9b732b15
2025-10-23 22:07:20.797  INFO 13140 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1761237440797
2025-10-23 22:07:20.804  INFO 13140 --- [           main] org.apache.kafka.streams.KafkaStreams    : stream-client [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd] State transition from CREATED to REBALANCING
2025-10-23 22:07:20.907  INFO 13140 --- [read-1-producer] org.apache.kafka.clients.Metadata        : [Producer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-producer] Cluster ID: sERge6jITumY8_DlrKc1Ug
2025-10-23 22:07:20.988  INFO 13140 --- [balStreamThread] o.a.k.s.s.i.RocksDBTimestampedStore      : Opening store path.treatment.sql.data.dedup-store in regular mode        
2025-10-23 22:07:20.989  INFO 13140 --- [balStreamThread] o.a.k.s.p.i.GlobalStateManagerImpl       : global-stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-GlobalStreamThread] Restoring state for global store path.treatment.sql.data.dedup-store
2025-10-23 22:07:20.995  INFO 13140 --- [balStreamThread] org.apache.kafka.clients.Metadata        : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Cluster ID: sERge6jITumY8_DlrKc1Ug
2025-10-23 22:07:21.011  INFO 13140 --- [balStreamThread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Subscribed to partition(s): path.treatment.sql.data.dedup-store-0
2025-10-23 22:07:21.013  INFO 13140 --- [balStreamThread] o.a.k.c.c.internals.SubscriptionState    : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Seeking to EARLIEST offset of partition path.treatment.sql.data.dedup-store-0
2025-10-23 22:07:21.019  INFO 13140 --- [balStreamThread] o.a.k.c.c.internals.SubscriptionState    : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Resetting offset for partition path.treatment.sql.data.dedup-store-0 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[01HW2133554:9092 (id: 0 rack: null)], epoch=0}}.   
2025-10-23 22:07:21.021  INFO 13140 --- [balStreamThread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Unsubscribed all topics or patterns and assigned partitions
2025-10-23 22:07:21.078  INFO 13140 --- [balStreamThread] o.a.k.s.s.i.RocksDBTimestampedStore      : Opening store path.medication.sql.data.dedup-store in regular mode       
2025-10-23 22:07:21.079  INFO 13140 --- [balStreamThread] o.a.k.s.p.i.GlobalStateManagerImpl       : global-stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-GlobalStreamThread] Restoring state for global store path.medication.sql.data.dedup-store
2025-10-23 22:07:21.209  INFO 13140 --- [balStreamThread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Subscribed to partition(s): path.medication.sql.data.dedup-store-0
2025-10-23 22:07:21.209  INFO 13140 --- [balStreamThread] o.a.k.c.c.internals.SubscriptionState    : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Seeking to EARLIEST offset of partition path.medication.sql.data.dedup-store-0
2025-10-23 22:07:21.212  INFO 13140 --- [balStreamThread] o.a.k.c.c.internals.SubscriptionState    : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Resetting offset for partition path.medication.sql.data.dedup-store-0 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[01HW2133554:9092 (id: 0 rack: null)], epoch=0}}.  
2025-10-23 22:07:21.212  INFO 13140 --- [balStreamThread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Unsubscribed all topics or patterns and assigned partitions
2025-10-23 22:07:21.217  INFO 13140 --- [balStreamThread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Subscribed to partition(s): path.medication.sql.data.dedup-store-0, path.treatment.sql.data.dedup-store-0
2025-10-23 22:07:21.218  INFO 13140 --- [balStreamThread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Seeking to offset 0 for partition path.medication.sql.data.dedup-store-0
2025-10-23 22:07:21.218  INFO 13140 --- [balStreamThread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Seeking to offset 0 for partition path.treatment.sql.data.dedup-store-0
2025-10-23 22:07:21.219  INFO 13140 --- [balStreamThread] o.a.k.s.p.internals.GlobalStreamThread   : global-stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-GlobalStreamThread] State transition from CREATED to RUNNING
2025-10-23 22:07:21.220  INFO 13140 --- [-StreamThread-1] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Starting
2025-10-23 22:07:21.221  INFO 13140 --- [-StreamThread-1] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] State transition from CREATED to STARTING
2025-10-23 22:07:21.221  INFO 13140 --- [-StreamThread-1] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Subscribed to topic(s): path.medication.sql.data, path.treatment.sql.data
2025-10-23 22:07:21.227  WARN 13140 --- [-StreamThread-1] org.apache.kafka.clients.NetworkClient   : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Error while fetching metadata with correlation id 2 : {path.medication.sql.data=UNKNOWN_TOPIC_OR_PARTITION}
2025-10-23 22:07:21.227  INFO 13140 --- [-StreamThread-1] org.apache.kafka.clients.Metadata        : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Cluster ID: sERge6jITumY8_DlrKc1Ug
2025-10-23 22:07:21.229  INFO 13140 --- [           main] c.p.stream.app.DedupStreamsApplication   : Started DedupStreamsApplication in 2.041 seconds (JVM running for 2.331) 
2025-10-23 22:07:21.232  INFO 13140 --- [-StreamThread-1] o.a.k.c.c.internals.AbstractCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Discovered group coordinator 01HW2133554:9092 (id: 2147483647 rack: null)
2025-10-23 22:07:21.235  INFO 13140 --- [-StreamThread-1] o.a.k.c.c.internals.AbstractCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] (Re-)joining group
2025-10-23 22:07:21.247  INFO 13140 --- [-StreamThread-1] o.a.k.c.c.internals.AbstractCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] (Re-)joining group
2025-10-23 22:07:21.253  INFO 13140 --- [-StreamThread-1] o.a.k.c.c.internals.AbstractCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Successfully joined group with generation Generation{generationId=1, memberId='dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer-9c6bb182-5f3c-47be-8893-eccda32d2878', protocol='stream'}
2025-10-23 22:07:21.341  WARN 13140 --- [-StreamThread-1] org.apache.kafka.clients.NetworkClient   : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Error while fetching metadata with correlation id 7 : {path.medication.sql.data=UNKNOWN_TOPIC_OR_PARTITION}
2025-10-23 22:07:21.344 ERROR 13140 --- [-StreamThread-1] o.a.k.s.p.internals.RepartitionTopics    : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer] The following source topics are missing/unknown: [path.medication.sql.data]. Please make sure all source topics have been pre-created before starting the Streams application.
2025-10-23 22:07:21.346 ERROR 13140 --- [-StreamThread-1] o.a.k.s.p.i.StreamsPartitionAssignor     : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer] Caught an error in the task assignment. Returning an error assignment.

org.apache.kafka.streams.errors.MissingSourceTopicException: Missing source topics.    
        at org.apache.kafka.streams.processor.internals.RepartitionTopics.checkIfExternalSourceTopicsExist(RepartitionTopics.java:124) ~[kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.RepartitionTopics.computeRepartitionTopicConfig(RepartitionTopics.java:98) ~[kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.RepartitionTopics.setup(RepartitionTopics.java:63) ~[kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.prepareRepartitionTopics(StreamsPartitionAssignor.java:486) ~[kafka-streams-2.8.1.jar:na]    
        at org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.assign(StreamsPartitionAssignor.java:365) ~[kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.performAssignment(ConsumerCoordinator.java:589) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.onJoinLeader(AbstractCoordinator.java:693) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.access$1000(AbstractCoordinator.java:111) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$JoinGroupResponseHandler.handle(AbstractCoordinator.java:599) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$JoinGroupResponseHandler.handle(AbstractCoordinator.java:562) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$CoordinatorResponseHandler.onSuccess(AbstractCoordinator.java:1182) [kafka-clients-2.8.1.jar:na]   
        at org.apache.kafka.clients.consumer.internals.AbstractCoordinator$CoordinatorResponseHandler.onSuccess(AbstractCoordinator.java:1157) [kafka-clients-2.8.1.jar:na]   
        at org.apache.kafka.clients.consumer.internals.RequestFuture$1.onSuccess(RequestFuture.java:206) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.RequestFuture.fireSuccess(RequestFuture.java:169) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.RequestFuture.complete(RequestFuture.java:129) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient$RequestFutureCompletionHandler.fireCompletion(ConsumerNetworkClient.java:602) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.firePendingCompletedRequests(ConsumerNetworkClient.java:412) [kafka-clients-2.8.1.jar:na]        
        at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.poll(ConsumerNetworkClient.java:297) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.poll(ConsumerNetworkClient.java:236) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.KafkaConsumer.pollForFetches(KafkaConsumer.java:1296) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1237) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1210) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamThread.pollRequests(StreamThread.java:925) [kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamThread.pollPhase(StreamThread.java:885) [kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamThread.runOnce(StreamThread.java:720) [kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamThread.runLoop(StreamThread.java:583) [kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamThread.run(StreamThread.java:556) [kafka-streams-2.8.1.jar:na]

2025-10-23 22:07:21.348  WARN 13140 --- [-StreamThread-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] The following subscribed topics are not assigned to any members: [path.medication.sql.data, path.treatment.sql.data]     
2025-10-23 22:07:21.349  INFO 13140 --- [-StreamThread-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Finished assignment for group at generation 1: {dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer-9c6bb182-5f3c-47be-8893-eccda32d2878=Assignment(partitions=[], userDataSize=40)} 
2025-10-23 22:07:21.354  INFO 13140 --- [-StreamThread-1] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Processed 0 total records, ran 0 punctuators, and committed 0 total tasks since the last update
2025-10-23 22:07:21.357  INFO 13140 --- [-StreamThread-1] o.a.k.c.c.internals.AbstractCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Successfully synced group in generation Generation{generationId=1, memberId='dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer-9c6bb182-5f3c-47be-8893-eccda32d2878', protocol='stream'}
2025-10-23 22:07:21.358  INFO 13140 --- [-StreamThread-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Updating assignment with
        Assigned partitions:                       []
        Current owned partitions:                  []
        Added partitions (assigned - owned):       []
        Revoked partitions (owned - assigned):     []

2025-10-23 22:07:21.358  INFO 13140 --- [-StreamThread-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Notifying assignor about the new Assignment(partitions=[], userDataSize=40)
2025-10-23 22:07:21.359  INFO 13140 --- [-StreamThread-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] Adding newly assigned partitions: 
2025-10-23 22:07:21.359 ERROR 13140 --- [-StreamThread-1] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Received error code INCOMPLETE_SOURCE_TOPIC_METADATA
2025-10-23 22:07:21.360 ERROR 13140 --- [-StreamThread-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer, groupId=dedup-streams-app] User provided listener org.apache.kafka.streams.processor.internals.StreamsRebalanceListener failed on invocation of onPartitionsAssigned for partitions []

org.apache.kafka.streams.errors.MissingSourceTopicException: One or more source topics were missing during rebalance
        at org.apache.kafka.streams.processor.internals.StreamsRebalanceListener.onPartitionsAssigned(StreamsRebalanceListener.java:58) ~[kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.invokePartitionsAssigned(ConsumerCoordinator.java:293) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.onJoinComplete(ConsumerCoordinator.java:430) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.joinGroupIfNeeded(AbstractCoordinator.java:449) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.ensureActiveGroup(AbstractCoordinator.java:365) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.poll(ConsumerCoordinator.java:508) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.KafkaConsumer.updateAssignmentMetadataIfNeeded(KafkaConsumer.java:1261) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1230) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1210) [kafka-clients-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamThread.pollRequests(StreamThread.java:925) [kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamThread.pollPhase(StreamThread.java:885) [kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamThread.runOnce(StreamThread.java:720) [kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamThread.runLoop(StreamThread.java:583) [kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.StreamThread.run(StreamThread.java:556) [kafka-streams-2.8.1.jar:na]

2025-10-23 22:07:21.361  INFO 13140 --- [-StreamThread-1] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] State transition from STARTING to PENDING_SHUTDOWN
2025-10-23 22:07:21.361  INFO 13140 --- [-StreamThread-1] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Shutting down
2025-10-23 22:07:21.363  INFO 13140 --- [-StreamThread-1] o.a.k.clients.producer.KafkaProducer     : [Producer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-producer] Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms.
2025-10-23 22:07:21.367  INFO 13140 --- [-StreamThread-1] org.apache.kafka.common.metrics.Metrics  : Metrics scheduler closed
2025-10-23 22:07:21.367  INFO 13140 --- [-StreamThread-1] org.apache.kafka.common.metrics.Metrics  : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-10-23 22:07:21.367  INFO 13140 --- [-StreamThread-1] org.apache.kafka.common.metrics.Metrics  : Metrics reporters closed
2025-10-23 22:07:21.368  INFO 13140 --- [-StreamThread-1] o.a.kafka.common.utils.AppInfoParser     : App info kafka.producer for dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-producer unregistered
2025-10-23 22:07:21.368  INFO 13140 --- [-StreamThread-1] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-restore-consumer, groupId=null] Unsubscribed all topics or patterns and assigned partitions
2025-10-23 22:07:21.369  INFO 13140 --- [-StreamThread-1] org.apache.kafka.common.metrics.Metrics  : Metrics scheduler closed
2025-10-23 22:07:21.369  INFO 13140 --- [-StreamThread-1] org.apache.kafka.common.metrics.Metrics  : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-10-23 22:07:21.369  INFO 13140 --- [-StreamThread-1] org.apache.kafka.common.metrics.Metrics  : Metrics reporters closed
2025-10-23 22:07:21.371  INFO 13140 --- [-StreamThread-1] o.a.kafka.common.utils.AppInfoParser     : App info kafka.consumer for dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer unregistered
2025-10-23 22:07:21.372  INFO 13140 --- [-StreamThread-1] org.apache.kafka.common.metrics.Metrics  : Metrics scheduler closed
2025-10-23 22:07:21.372  INFO 13140 --- [-StreamThread-1] org.apache.kafka.common.metrics.Metrics  : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-10-23 22:07:21.372  INFO 13140 --- [-StreamThread-1] org.apache.kafka.common.metrics.Metrics  : Metrics reporters closed
2025-10-23 22:07:21.373  INFO 13140 --- [-StreamThread-1] o.a.kafka.common.utils.AppInfoParser     : App info kafka.consumer for dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-restore-consumer unregistered
2025-10-23 22:07:21.374  INFO 13140 --- [-StreamThread-1] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] State transition from PENDING_SHUTDOWN to DEAD
2025-10-23 22:07:21.374  INFO 13140 --- [-StreamThread-1] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Shutdown complete
Exception in thread "dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1" org.apache.kafka.streams.errors.MissingSourceTopicException: One or more source topics were missing during rebalance
        at org.apache.kafka.streams.processor.internals.StreamsRebalanceListener.onPartitionsAssigned(StreamsRebalanceListener.java:58)
        at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.invokePartitionsAssigned(ConsumerCoordinator.java:293)
        at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.onJoinComplete(ConsumerCoordinator.java:430)
        at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.joinGroupIfNeeded(AbstractCoordinator.java:449)
        at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.ensureActiveGroup(AbstractCoordinator.java:365)
        at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.poll(ConsumerCoordinator.java:508)
        at org.apache.kafka.clients.consumer.KafkaConsumer.updateAssignmentMetadataIfNeeded(KafkaConsumer.java:1261)
        at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1230)
        at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1210)
        at org.apache.kafka.streams.processor.internals.StreamThread.pollRequests(StreamThread.java:925)
        at org.apache.kafka.streams.processor.internals.StreamThread.pollPhase(StreamThread.java:885)
        at org.apache.kafka.streams.processor.internals.StreamThread.runOnce(StreamThread.java:720)
        at org.apache.kafka.streams.processor.internals.StreamThread.runLoop(StreamThread.java:583)
        at org.apache.kafka.streams.processor.internals.StreamThread.run(StreamThread.java:556)