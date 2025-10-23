
2025-10-23 21:58:10.924  INFO 27312 --- [           main] c.p.stream.app.DedupStreamsApplication   : Starting DedupStreamsApplication using Java 1.8.0_302 on 01HW2133554 with PID 27312 (C:\Temp\MT_REPOS\path-kafka-internal-custom-connectors\path-kafka-streams\target\classes started by 2921892 in C:\Temp\MT_REPOS\path-kafka-internal-custom-connectors\path-kafka-streams)
2025-10-23 21:58:10.929  INFO 27312 --- [           main] c.p.stream.app.DedupStreamsApplication   : No active profile set, falling back to 1 default profile: "default"
2025-10-23 21:58:11.673  INFO 27312 --- [           main] org.apache.kafka.streams.StreamsConfig   : StreamsConfig values:
        acceptable.recovery.lag = 10000
        application.id = dedup-streams-app
        application.server =
        bootstrap.servers = [localhost:9092]
        buffered.records.per.partition = 1000
        built.in.metrics.version = latest
        cache.max.bytes.buffering = 10485760
        client.id =
        commit.interval.ms = 30000
        connections.max.idle.ms = 540000
        default.deserialization.exception.handler = class org.apache.kafka.streams.errors.LogAndFailExceptionHandler
        default.key.serde = class org.apache.kafka.common.serialization.Serdes$StringSerde
        default.production.exception.handler = class org.apache.kafka.streams.errors.DefaultProductionExceptionHandler
        default.timestamp.extractor = class org.apache.kafka.streams.processor.FailOnInvalidTimestamp
        default.value.serde = class org.apache.kafka.common.serialization.Serdes$StringSerde
        default.windowed.key.serde.inner = null
        default.windowed.value.serde.inner = null
        max.task.idle.ms = 0
        max.warmup.replicas = 2
        metadata.max.age.ms = 300000
        metric.reporters = []
        metrics.num.samples = 2
        metrics.recording.level = INFO
        metrics.sample.window.ms = 30000
        num.standby.replicas = 0
        num.stream.threads = 1
        partition.grouper = class org.apache.kafka.streams.processor.DefaultPartitionGrouper
        poll.ms = 100
        probing.rebalance.interval.ms = 600000
        processing.guarantee = at_least_once
        receive.buffer.bytes = 32768
        reconnect.backoff.max.ms = 1000
        reconnect.backoff.ms = 50
        replication.factor = 1
        request.timeout.ms = 40000
        retries = 0
        retry.backoff.ms = 100
        rocksdb.config.setter = null
        security.protocol = PLAINTEXT
        send.buffer.bytes = 131072
        state.cleanup.delay.ms = 600000
        state.dir = C:\Users\2921892\AppData\Local\Temp\\kafka-streams        
        task.timeout.ms = 300000
        topology.optimization = none
        upgrade.from = null
        window.size.ms = null
        windowstore.changelog.additional.retention.ms = 86400000

2025-10-23 21:58:11.682  WARN 27312 --- [           main] o.a.k.s.p.internals.StateDirectory       : Using an OS temp directory in the state.dir property can cause failures with writing the checkpoint file due to the fact that this directory can be cleared by the OS. Resolved state.dir: [C:\Users\2921892\AppData\Local\Temp\\kafka-streams]
2025-10-23 21:58:11.682 ERROR 27312 --- [           main] o.a.k.s.p.internals.StateDirectory       : Failed to change permissions for the directory C:\Users\2921892\AppData\Local\Temp\kafka-streams
2025-10-23 21:58:11.683 ERROR 27312 --- [           main] o.a.k.s.p.internals.StateDirectory       : Failed to change permissions for the directory C:\Users\2921892\AppData\Local\Temp\kafka-streams\dedup-streams-app
2025-10-23 21:58:12.071  INFO 27312 --- [           main] o.a.k.s.p.internals.StateDirectory       : No process id found on disk, got fresh process id 6866afaf-71d8-4893-b0d8-50053832a6cd
2025-10-23 21:58:12.111  INFO 27312 --- [           main] o.a.k.clients.admin.AdminClientConfig    : AdminClientConfig values:
        bootstrap.servers = [localhost:9092]
        client.dns.lookup = use_all_dns_ips
        client.id = dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-admin
        connections.max.idle.ms = 300000
        default.api.timeout.ms = 60000
        metadata.max.age.ms = 300000
        metric.reporters = []
        metrics.num.samples = 2
        metrics.recording.level = INFO
        metrics.sample.window.ms = 30000
        receive.buffer.bytes = 65536
        reconnect.backoff.max.ms = 1000
        reconnect.backoff.ms = 50
        request.timeout.ms = 30000
        retries = 2147483647
        retry.backoff.ms = 100
        sasl.client.callback.handler.class = null
        sasl.jaas.config = null
        sasl.kerberos.kinit.cmd = /usr/bin/kinit
        sasl.kerberos.min.time.before.relogin = 60000
        sasl.kerberos.service.name = null
        sasl.kerberos.ticket.renew.jitter = 0.05
        sasl.kerberos.ticket.renew.window.factor = 0.8
        sasl.login.callback.handler.class = null
        sasl.login.class = null
        sasl.login.refresh.buffer.seconds = 300
        sasl.login.refresh.min.period.seconds = 60
        sasl.login.refresh.window.factor = 0.8
        sasl.login.refresh.window.jitter = 0.05
        sasl.mechanism = GSSAPI
        security.protocol = PLAINTEXT
        security.providers = null
        send.buffer.bytes = 131072
        socket.connection.setup.timeout.max.ms = 30000
        socket.connection.setup.timeout.ms = 10000
        ssl.cipher.suites = null
        ssl.enabled.protocols = [TLSv1.2]
        ssl.endpoint.identification.algorithm = https
        ssl.engine.factory.class = null
        ssl.key.password = null
        ssl.keymanager.algorithm = SunX509
        ssl.keystore.certificate.chain = null
        ssl.keystore.key = null
        ssl.keystore.location = null
        ssl.keystore.password = null
        ssl.keystore.type = JKS
        ssl.protocol = TLSv1.2
        ssl.provider = null
        ssl.secure.random.implementation = null
        ssl.trustmanager.algorithm = PKIX
        ssl.truststore.certificates = null
        ssl.truststore.location = null
        ssl.truststore.password = null
        ssl.truststore.type = JKS

2025-10-23 21:58:12.177  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.0.2
2025-10-23 21:58:12.177  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: 25b1aea02e37da14
2025-10-23 21:58:12.178  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1761236892176
2025-10-23 21:58:12.180  INFO 27312 --- [           main] org.apache.kafka.streams.KafkaStreams    : stream-client [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd] Kafka Streams version: 2.8.1
2025-10-23 21:58:12.180  INFO 27312 --- [           main] org.apache.kafka.streams.KafkaStreams    : stream-client [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd] Kafka Streams commit ID: 839b886f9b732b15
2025-10-23 21:58:12.191  INFO 27312 --- [           main] o.a.k.clients.consumer.ConsumerConfig    : ConsumerConfig values:
        allow.auto.create.topics = true
        auto.commit.interval.ms = 5000
        auto.offset.reset = none
        bootstrap.servers = [localhost:9092]
        check.crcs = true
        client.dns.lookup = use_all_dns_ips
        client.id = dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-global-consumer
        client.rack =
        connections.max.idle.ms = 540000
        default.api.timeout.ms = 60000
        enable.auto.commit = false
        exclude.internal.topics = true
        fetch.max.bytes = 52428800
        fetch.max.wait.ms = 500
        fetch.min.bytes = 1
        group.id = null
        group.instance.id = null
        heartbeat.interval.ms = 3000
        interceptor.classes = []
        internal.leave.group.on.close = false
        internal.throw.on.fetch.stable.offset.unsupported = false
        isolation.level = read_uncommitted
        key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        max.partition.fetch.bytes = 1048576
        max.poll.interval.ms = 300000
        max.poll.records = 1000
        metadata.max.age.ms = 300000
        metric.reporters = []
        metrics.num.samples = 2
        metrics.recording.level = INFO
        metrics.sample.window.ms = 30000
        partition.assignment.strategy = [class org.apache.kafka.clients.consumer.RangeAssignor, class org.apache.kafka.clients.consumer.CooperativeStickyAssignor]
        receive.buffer.bytes = 65536
        reconnect.backoff.max.ms = 1000
        reconnect.backoff.ms = 50
        request.timeout.ms = 30000
        retry.backoff.ms = 100
        sasl.client.callback.handler.class = null
        sasl.jaas.config = null
        sasl.kerberos.kinit.cmd = /usr/bin/kinit
        sasl.kerberos.min.time.before.relogin = 60000
        sasl.kerberos.service.name = null
        sasl.kerberos.ticket.renew.jitter = 0.05
        sasl.kerberos.ticket.renew.window.factor = 0.8
        sasl.login.callback.handler.class = null
        sasl.login.class = null
        sasl.login.refresh.buffer.seconds = 300
        sasl.login.refresh.min.period.seconds = 60
        sasl.login.refresh.window.factor = 0.8
        sasl.login.refresh.window.jitter = 0.05
        sasl.mechanism = GSSAPI
        security.protocol = PLAINTEXT
        security.providers = null
        send.buffer.bytes = 131072
        session.timeout.ms = 45000
        socket.connection.setup.timeout.max.ms = 30000
        socket.connection.setup.timeout.ms = 10000
        ssl.cipher.suites = null
        ssl.enabled.protocols = [TLSv1.2]
        ssl.endpoint.identification.algorithm = https
        ssl.engine.factory.class = null
        ssl.key.password = null
        ssl.keymanager.algorithm = SunX509
        ssl.keystore.certificate.chain = null
        ssl.keystore.key = null
        ssl.keystore.location = null
        ssl.keystore.password = null
        ssl.keystore.type = JKS
        ssl.protocol = TLSv1.2
        ssl.provider = null
        ssl.secure.random.implementation = null
        ssl.trustmanager.algorithm = PKIX
        ssl.truststore.certificates = null
        ssl.truststore.location = null
        ssl.truststore.password = null
        ssl.truststore.type = JKS
        value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer

2025-10-23 21:58:12.210  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.0.2
2025-10-23 21:58:12.210  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: 25b1aea02e37da14
2025-10-23 21:58:12.210  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1761236892210
2025-10-23 21:58:12.215  INFO 27312 --- [           main] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Creating restore consumer client
2025-10-23 21:58:12.231  INFO 27312 --- [           main] o.a.k.clients.consumer.ConsumerConfig    : ConsumerConfig values:
        allow.auto.create.topics = true
        auto.commit.interval.ms = 5000
        auto.offset.reset = none
        bootstrap.servers = [localhost:9092]
        check.crcs = true
        client.dns.lookup = use_all_dns_ips
        client.id = dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-restore-consumer
        client.rack =
        connections.max.idle.ms = 540000
        default.api.timeout.ms = 60000
        enable.auto.commit = false
        exclude.internal.topics = true
        fetch.max.bytes = 52428800
        fetch.max.wait.ms = 500
        fetch.min.bytes = 1
        group.id = null
        group.instance.id = null
        heartbeat.interval.ms = 3000
        interceptor.classes = []
        internal.leave.group.on.close = false
        internal.throw.on.fetch.stable.offset.unsupported = false
        isolation.level = read_uncommitted
        key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        max.partition.fetch.bytes = 1048576
        max.poll.interval.ms = 300000
        max.poll.records = 1000
        metadata.max.age.ms = 300000
        metric.reporters = []
        metrics.num.samples = 2
        metrics.recording.level = INFO
        metrics.sample.window.ms = 30000
        partition.assignment.strategy = [class org.apache.kafka.clients.consumer.RangeAssignor, class org.apache.kafka.clients.consumer.CooperativeStickyAssignor]
        receive.buffer.bytes = 65536
        reconnect.backoff.max.ms = 1000
        reconnect.backoff.ms = 50
        request.timeout.ms = 30000
        retry.backoff.ms = 100
        sasl.client.callback.handler.class = null
        sasl.jaas.config = null
        sasl.kerberos.kinit.cmd = /usr/bin/kinit
        sasl.kerberos.min.time.before.relogin = 60000
        sasl.kerberos.service.name = null
        sasl.kerberos.ticket.renew.jitter = 0.05
        sasl.kerberos.ticket.renew.window.factor = 0.8
        sasl.login.callback.handler.class = null
        sasl.login.class = null
        sasl.login.refresh.buffer.seconds = 300
        sasl.login.refresh.min.period.seconds = 60
        sasl.login.refresh.window.factor = 0.8
        sasl.login.refresh.window.jitter = 0.05
        sasl.mechanism = GSSAPI
        security.protocol = PLAINTEXT
        security.providers = null
        send.buffer.bytes = 131072
        session.timeout.ms = 45000
        socket.connection.setup.timeout.max.ms = 30000
        socket.connection.setup.timeout.ms = 10000
        ssl.cipher.suites = null
        ssl.enabled.protocols = [TLSv1.2]
        ssl.endpoint.identification.algorithm = https
        ssl.engine.factory.class = null
        ssl.key.password = null
        ssl.keymanager.algorithm = SunX509
        ssl.keystore.certificate.chain = null
        ssl.keystore.key = null
        ssl.keystore.location = null
        ssl.keystore.password = null
        ssl.keystore.type = JKS
        ssl.protocol = TLSv1.2
        ssl.provider = null
        ssl.secure.random.implementation = null
        ssl.trustmanager.algorithm = PKIX
        ssl.truststore.certificates = null
        ssl.truststore.location = null
        ssl.truststore.password = null
        ssl.truststore.type = JKS
        value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer

2025-10-23 21:58:12.249  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.0.2
2025-10-23 21:58:12.249  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: 25b1aea02e37da14
2025-10-23 21:58:12.249  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1761236892249
2025-10-23 21:58:12.254  INFO 27312 --- [           main] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Creating thread producer client
2025-10-23 21:58:12.258  INFO 27312 --- [           main] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values:
        acks = -1
        batch.size = 16384
        bootstrap.servers = [localhost:9092]
        buffer.memory = 33554432
        client.dns.lookup = use_all_dns_ips
        client.id = dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-producer
        compression.type = none
        connections.max.idle.ms = 540000
        delivery.timeout.ms = 120000
        enable.idempotence = true
        interceptor.classes = []
        key.serializer = class org.apache.kafka.common.serialization.ByteArraySerializer
        linger.ms = 100
        max.block.ms = 60000
        max.in.flight.requests.per.connection = 5
        max.request.size = 1048576
        metadata.max.age.ms = 300000
        metadata.max.idle.ms = 300000
        metric.reporters = []
        metrics.num.samples = 2
        metrics.recording.level = INFO
        metrics.sample.window.ms = 30000
        partitioner.class = class org.apache.kafka.clients.producer.internals.DefaultPartitioner
        receive.buffer.bytes = 32768
        reconnect.backoff.max.ms = 1000
        reconnect.backoff.ms = 50
        request.timeout.ms = 30000
        retries = 2147483647
        retry.backoff.ms = 100
        sasl.client.callback.handler.class = null
        sasl.jaas.config = null
        sasl.kerberos.kinit.cmd = /usr/bin/kinit
        sasl.kerberos.min.time.before.relogin = 60000
        sasl.kerberos.service.name = null
        sasl.kerberos.ticket.renew.jitter = 0.05
        sasl.kerberos.ticket.renew.window.factor = 0.8
        sasl.login.callback.handler.class = null
        sasl.login.class = null
        sasl.login.refresh.buffer.seconds = 300
        sasl.login.refresh.min.period.seconds = 60
        sasl.login.refresh.window.factor = 0.8
        sasl.login.refresh.window.jitter = 0.05
        sasl.mechanism = GSSAPI
        security.protocol = PLAINTEXT
        security.providers = null
        send.buffer.bytes = 131072
        socket.connection.setup.timeout.max.ms = 30000
        socket.connection.setup.timeout.ms = 10000
        ssl.cipher.suites = null
        ssl.enabled.protocols = [TLSv1.2]
        ssl.endpoint.identification.algorithm = https
        ssl.engine.factory.class = null
        ssl.key.password = null
        ssl.keymanager.algorithm = SunX509
        ssl.keystore.certificate.chain = null
        ssl.keystore.key = null
        ssl.keystore.location = null
        ssl.keystore.password = null
        ssl.keystore.type = JKS
        ssl.protocol = TLSv1.2
        ssl.provider = null
        ssl.secure.random.implementation = null
        ssl.trustmanager.algorithm = PKIX
        ssl.truststore.certificates = null
        ssl.truststore.location = null
        ssl.truststore.password = null
        ssl.truststore.type = JKS
        transaction.timeout.ms = 60000
        transactional.id = null
        value.serializer = class org.apache.kafka.common.serialization.ByteArraySerializer

2025-10-23 21:58:12.263  INFO 27312 --- [           main] o.a.k.clients.producer.KafkaProducer     : [Producer clientId=dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-producer] Instantiated an idempotent producer.
2025-10-23 21:58:12.284  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.0.2
2025-10-23 21:58:12.284  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: 25b1aea02e37da14
2025-10-23 21:58:12.285  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1761236892284
2025-10-23 21:58:12.287  INFO 27312 --- [           main] o.a.k.s.p.internals.StreamThread         : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1] Creating consumer client
2025-10-23 21:58:12.288  INFO 27312 --- [           main] o.a.k.clients.consumer.ConsumerConfig    : ConsumerConfig values:
        allow.auto.create.topics = false
        auto.commit.interval.ms = 5000
        auto.offset.reset = earliest
        bootstrap.servers = [localhost:9092]
        check.crcs = true
        client.dns.lookup = use_all_dns_ips
        client.id = dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer
        client.rack =
        connections.max.idle.ms = 540000
        default.api.timeout.ms = 60000
        enable.auto.commit = false
        exclude.internal.topics = true
        fetch.max.bytes = 52428800
        fetch.max.wait.ms = 500
        fetch.min.bytes = 1
        group.id = dedup-streams-app
        group.instance.id = null
        heartbeat.interval.ms = 3000
        interceptor.classes = []
        internal.leave.group.on.close = false
        internal.throw.on.fetch.stable.offset.unsupported = false
        isolation.level = read_uncommitted
        key.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer
        max.partition.fetch.bytes = 1048576
        max.poll.interval.ms = 300000
        max.poll.records = 1000
        metadata.max.age.ms = 300000
        metric.reporters = []
        metrics.num.samples = 2
        metrics.recording.level = INFO
        metrics.sample.window.ms = 30000
        partition.assignment.strategy = [org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor]
        receive.buffer.bytes = 65536
        reconnect.backoff.max.ms = 1000
        reconnect.backoff.ms = 50
        request.timeout.ms = 30000
        retry.backoff.ms = 100
        sasl.client.callback.handler.class = null
        sasl.jaas.config = null
        sasl.kerberos.kinit.cmd = /usr/bin/kinit
        sasl.kerberos.min.time.before.relogin = 60000
        sasl.kerberos.service.name = null
        sasl.kerberos.ticket.renew.jitter = 0.05
        sasl.kerberos.ticket.renew.window.factor = 0.8
        sasl.login.callback.handler.class = null
        sasl.login.class = null
        sasl.login.refresh.buffer.seconds = 300
        sasl.login.refresh.min.period.seconds = 60
        sasl.login.refresh.window.factor = 0.8
        sasl.login.refresh.window.jitter = 0.05
        sasl.mechanism = GSSAPI
        security.protocol = PLAINTEXT
        security.providers = null
        send.buffer.bytes = 131072
        session.timeout.ms = 45000
        socket.connection.setup.timeout.max.ms = 30000
        socket.connection.setup.timeout.ms = 10000
        ssl.cipher.suites = null
        ssl.enabled.protocols = [TLSv1.2]
        ssl.endpoint.identification.algorithm = https
        ssl.engine.factory.class = null
        ssl.key.password = null
        ssl.keymanager.algorithm = SunX509
        ssl.keystore.certificate.chain = null
        ssl.keystore.key = null
        ssl.keystore.location = null
        ssl.keystore.password = null
        ssl.keystore.type = JKS
        ssl.protocol = TLSv1.2
        ssl.provider = null
        ssl.secure.random.implementation = null
        ssl.trustmanager.algorithm = PKIX
        ssl.truststore.certificates = null
        ssl.truststore.location = null
        ssl.truststore.password = null
        ssl.truststore.type = JKS
        value.deserializer = class org.apache.kafka.common.serialization.ByteArrayDeserializer

2025-10-23 21:58:12.297  INFO 27312 --- [           main] o.a.k.s.p.i.a.AssignorConfiguration      : stream-thread [dedup-streams-app-6866afaf-71d8-4893-b0d8-50053832a6cd-StreamThread-1-consumer] Cooperative rebalancing enabled now   
2025-10-23 21:58:12.304  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.0.2
2025-10-23 21:58:12.304  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: 25b1aea02e37da14
2025-10-23 21:58:12.305  INFO 27312 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1761236892304
2025-10-23 21:58:12.308  WARN 27312 --- [           main] s.c.a.AnnotationConfigApplicationContext : Exception encountered during context initialization - cancelling refresh attempt: org.springframework.context.ApplicationContextException: Failed to start bean 'defaultKafkaStreamsBuilder'; nested exception is java.lang.NoSuchMethodError: org.apache.kafka.clients.consumer.ConsumerConfig.addDeserializerToConfig(Ljava/util/Map;Lorg/apache/kafka/common/serialization/Deserializer;Lorg/apache/kafka/common/serialization/Deserializer;)Ljava/util/Map;
2025-10-23 21:58:12.316  INFO 27312 --- [           main] ConditionEvaluationReportLoggingListener :

Error starting ApplicationContext. To display the conditions report re-run your application with 'debug' enabled.
2025-10-23 21:58:12.333 ERROR 27312 --- [           main] o.s.b.d.LoggingFailureAnalysisReporter   :

***************************
APPLICATION FAILED TO START
***************************

Description:

An attempt was made to call a method that does not exist. The attempt was made from the following location:

    org.apache.kafka.streams.processor.internals.StreamThread$InternalConsumerConfig.<init>(StreamThread.java:537)

The following method did not exist:

    org.apache.kafka.clients.consumer.ConsumerConfig.addDeserializerToConfig(Ljava/util/Map;Lorg/apache/kafka/common/serialization/Deserializer;Lorg/apache/kafka/common/serialization/Deserializer;)Ljava/util/Map;

The calling method's class, org.apache.kafka.streams.processor.internals.StreamThread$InternalConsumerConfig, was loaded from the following location:       

    jar:file:/C:/Users/2921892/.m2/repository/org/apache/kafka/kafka-streams/2.8.1/kafka-streams-2.8.1.jar!/org/apache/kafka/streams/processor/internals/StreamThread$InternalConsumerConfig.class

The called method's class, org.apache.kafka.clients.consumer.ConsumerConfig, is available from the following locations:

    jar:file:/C:/Users/2921892/.m2/repository/org/apache/kafka/kafka-clients/3.0.2/kafka-clients-3.0.2.jar!/org/apache/kafka/clients/consumer/ConsumerConfig.class

The called method's class hierarchy was loaded from the following locations:  

    org.apache.kafka.clients.consumer.ConsumerConfig: file:/C:/Users/2921892/.m2/repository/org/apache/kafka/kafka-clients/3.0.2/kafka-clients-3.0.2.jar    
    org.apache.kafka.common.config.AbstractConfig: file:/C:/Users/2921892/.m2/repository/org/apache/kafka/kafka-clients/3.0.2/kafka-clients-3.0.2.jar       


Action:

Correct the classpath of your application so that it contains compatible versions of the classes org.apache.kafka.streams.processor.internals.StreamThread$InternalConsumerConfig and org.apache.kafka.clients.consumer.ConsumerConfig    