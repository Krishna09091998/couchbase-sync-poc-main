2025-10-23 22:14:15.185  INFO 5556 --- [read-1-producer] org.apache.kafka.clients.Metadata        : [Producer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-producer] Cluster ID: sERge6jITumY8_DlrKc1Ug
2025-10-23 22:14:15.242  INFO 5556 --- [balStreamThread] o.a.k.s.s.i.RocksDBTimestampedStore      : Opening store path.treatment.sql.data.dedup-store in regular mode
2025-10-23 22:14:15.244  INFO 5556 --- [balStreamThread] o.a.k.s.p.i.GlobalStateManagerImpl       : global-stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-GlobalStreamThread] Restoring state for global store path.treatment.sql.data.dedup-store
2025-10-23 22:14:15.250  INFO 5556 --- [balStreamThread] org.apache.kafka.clients.Metadata        : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Cluster ID: sERge6jITumY8_DlrKc1Ug
2025-10-23 22:14:15.266  INFO 5556 --- [balStreamThread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Subscribed to partition(s): path.treatment.sql.data.dedup-store-0
2025-10-23 22:14:15.268  INFO 5556 --- [balStreamThread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Seeking to offset 0 for partition path.treatment.sql.data.dedup-store-0
2025-10-23 22:14:15.269  INFO 5556 --- [balStreamThread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer, groupId=null] Unsubscribed all topics or patterns and assigned partitions
2025-10-23 22:14:15.270 ERROR 5556 --- [balStreamThread] o.a.k.s.p.i.GlobalStateManagerImpl       : global-stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-GlobalStreamThread] Encountered a topic-partition in the global checkpoint file not associated with any global state store, topic-partition: path.medication.sql.data.dedup-store, checkpoint file: C:\Users\2921892\AppData\Local\Temp\kafka-streams\dedup-streams-app\global\.checkpoint. If this topic-partition is no longer valid, an application reset and state store directory cleanup will be required.
2025-10-23 22:14:15.271  INFO 5556 --- [balStreamThread] o.a.k.s.p.internals.GlobalStreamThread   : global-stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-GlobalStreamThread] State transition from CREATED to PENDING_SHUTDOWN
2025-10-23 22:14:15.271  INFO 5556 --- [balStreamThread] o.a.k.s.p.internals.GlobalStreamThread   : global-stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-GlobalStreamThread] State transition from PENDING_SHUTDOWN to DEAD
2025-10-23 22:14:15.271 ERROR 5556 --- [balStreamThread] org.apache.kafka.streams.KafkaStreams    : stream-client [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd] Global thread has died. The streams application or client will now close to ERROR.        
2025-10-23 22:14:15.271  INFO 5556 --- [balStreamThread] org.apache.kafka.streams.KafkaStreams    : stream-client [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd] State transition from REBALANCING to PENDING_ERROR
2025-10-23 22:14:15.272  WARN 5556 --- [balStreamThread] o.a.k.s.p.internals.GlobalStreamThread   : global-stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-GlobalStreamThread] Error happened during initialization of the global state store; this thread has shutdown
2025-10-23 22:14:15.273  INFO 5556 --- [ms-close-thread] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Informed to shut down
2025-10-23 22:14:15.273  INFO 5556 --- [ms-close-thread] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] State transition from CREATED to PENDING_SHUTDOWN
2025-10-23 22:14:15.274  INFO 5556 --- [ms-close-thread] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Shutting down
2025-10-23 22:14:15.274  WARN 5556 --- [           main] s.c.a.AnnotationConfigApplicationContext : Exception encountered during context initialization - cancelling refresh attempt: org.springframework.context.ApplicationContextException: Failed to start bean 'defaultKafkaStreamsBuilder'; nested exception is org.springframework.kafka.KafkaException: Could not start stream: ; nested exception is org.apache.kafka.streams.errors.StreamsException: Encountered a topic-partition not associated with any global state store  
2025-10-23 22:14:15.275  INFO 5556 --- [ms-close-thread] o.a.k.clients.producer.KafkaProducer     : [Producer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-producer] Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms.
2025-10-23 22:14:15.279  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Metrics scheduler closed
2025-10-23 22:14:15.280  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-10-23 22:14:15.280  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Metrics reporters closed
2025-10-23 22:14:15.280  INFO 5556 --- [ms-close-thread] o.a.kafka.common.utils.AppInfoParser     : App info kafka.producer for dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-producer unregistered
2025-10-23 22:14:15.281  INFO 5556 --- [ms-close-thread] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-restore-consumer, groupId=null] Unsubscribed all topics or patterns and assigned partitions
2025-10-23 22:14:15.282  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Metrics scheduler closed
2025-10-23 22:14:15.282  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-10-23 22:14:15.282  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Metrics reporters closed
2025-10-23 22:14:15.284  INFO 5556 --- [ms-close-thread] o.a.kafka.common.utils.AppInfoParser     : App info kafka.consumer for dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer unregistered
2025-10-23 22:14:15.284  INFO 5556 --- [           main] ConditionEvaluationReportLoggingListener :

Error starting ApplicationContext. To display the conditions report re-run your application with 'debug' enabled.
2025-10-23 22:14:15.284  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Metrics scheduler closed
2025-10-23 22:14:15.284  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-10-23 22:14:15.284  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Metrics reporters closed
2025-10-23 22:14:15.285  INFO 5556 --- [ms-close-thread] o.a.kafka.common.utils.AppInfoParser     : App info kafka.consumer for dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-restore-consumer unregistered
2025-10-23 22:14:15.286  INFO 5556 --- [ms-close-thread] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] State transition from PENDING_SHUTDOWN to DEAD
2025-10-23 22:14:15.287  INFO 5556 --- [ms-close-thread] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Shutdown complete
2025-10-23 22:14:15.288  INFO 5556 --- [53832a6cd-admin] o.a.kafka.common.utils.AppInfoParser     : App info kafka.admin.client for dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-admin unregistered
2025-10-23 22:14:15.289  INFO 5556 --- [53832a6cd-admin] org.apache.kafka.common.metrics.Metrics  : Metrics scheduler closed
2025-10-23 22:14:15.290  INFO 5556 --- [53832a6cd-admin] org.apache.kafka.common.metrics.Metrics  : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-10-23 22:14:15.290  INFO 5556 --- [53832a6cd-admin] org.apache.kafka.common.metrics.Metrics  : Metrics reporters closed
2025-10-23 22:14:15.290  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Metrics scheduler closed
2025-10-23 22:14:15.291  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-10-23 22:14:15.291  INFO 5556 --- [ms-close-thread] org.apache.kafka.common.metrics.Metrics  : Metrics reporters closed
2025-10-23 22:14:15.291  INFO 5556 --- [ms-close-thread] org.apache.kafka.streams.KafkaStreams    : stream-client [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd] State transition from PENDING_ERROR to ERROR
2025-10-23 22:14:15.304 ERROR 5556 --- [           main] o.s.boot.SpringApplication               : Application run failed

org.springframework.context.ApplicationContextException: Failed to start bean 'defaultKafkaStreamsBuilder'; nested exception is org.springframework.kafka.KafkaException: Could not start stream: ; nested exception is org.apache.kafka.streams.errors.StreamsException: Encountered a topic-partition not associated with any global state store
        at org.springframework.context.support.DefaultLifecycleProcessor.doStart(DefaultLifecycleProcessor.java:181) ~[spring-context-5.3.24.jar:5.3.24]
        at org.springframework.context.support.DefaultLifecycleProcessor.access$200(DefaultLifecycleProcessor.java:54) ~[spring-context-5.3.24.jar:5.3.24]
        at org.springframework.context.support.DefaultLifecycleProcessor$LifecycleGroup.start(DefaultLifecycleProcessor.java:356) ~[spring-context-5.3.24.jar:5.3.24]
        at java.lang.Iterable.forEach(Iterable.java:75) ~[na:1.8.0_302]
        at org.springframework.context.support.DefaultLifecycleProcessor.startBeans(DefaultLifecycleProcessor.java:155) ~[spring-context-5.3.24.jar:5.3.24]
        at org.springframework.context.support.DefaultLifecycleProcessor.onRefresh(DefaultLifecycleProcessor.java:123) ~[spring-context-5.3.24.jar:5.3.24]
        at org.springframework.context.support.AbstractApplicationContext.finishRefresh(AbstractApplicationContext.java:935) ~[spring-context-5.3.24.jar:5.3.24]
        at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:586) ~[spring-context-5.3.24.jar:5.3.24]
        at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:732) [spring-boot-2.7.18.jar:2.7.18]
        at org.springframework.boot.SpringApplication.refreshContext(SpringApplication.java:409) [spring-boot-2.7.18.jar:2.7.18]
        at org.springframework.boot.SpringApplication.run(SpringApplication.java:308) [spring-boot-2.7.18.jar:2.7.18]
        at org.springframework.boot.SpringApplication.run(SpringApplication.java:1300) [spring-boot-2.7.18.jar:2.7.18]
        at org.springframework.boot.SpringApplication.run(SpringApplication.java:1289) [spring-boot-2.7.18.jar:2.7.18]
        at com.path.stream.app.DedupStreamsApplication.main(DedupStreamsApplication.java:30) [classes/:na]
Caused by: org.springframework.kafka.KafkaException: Could not start stream: ; nested exception is org.apache.kafka.streams.errors.StreamsException: Encountered a topic-partition not associated with any global state store
        at org.springframework.kafka.config.StreamsBuilderFactoryBean.start(StreamsBuilderFactoryBean.java:371) ~[spring-kafka-2.8.11.jar:2.8.11]
        at org.springframework.context.support.DefaultLifecycleProcessor.doStart(DefaultLifecycleProcessor.java:178) ~[spring-context-5.3.24.jar:5.3.24]
        ... 13 common frames omitted
Caused by: org.apache.kafka.streams.errors.StreamsException: Encountered a topic-partition not associated with any global state store
        at org.apache.kafka.streams.processor.internals.GlobalStateManagerImpl.lambda$initialize$0(GlobalStateManagerImpl.java:170) ~[kafka-streams-2.8.1.jar:na]
        at java.util.HashMap$KeySet.forEach(HashMap.java:933) ~[na:1.8.0_302]
        at org.apache.kafka.streams.processor.internals.GlobalStateManagerImpl.initialize(GlobalStateManagerImpl.java:156) ~[kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.GlobalStateUpdateTask.initialize(GlobalStateUpdateTask.java:68) ~[kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.GlobalStreamThread$StateConsumer.initialize(GlobalStreamThread.java:252) ~[kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.GlobalStreamThread.initialize(GlobalStreamThread.java:393) ~[kafka-streams-2.8.1.jar:na]
        at org.apache.kafka.streams.processor.internals.GlobalStreamThread.run(GlobalStreamThread.java:287) ~[kafka-streams-2.8.1.jar:na]

[INFO] ------------------------------------------------------------------------