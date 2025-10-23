2025-10-23 21:43:28.976  INFO 26832 --- [           main] c.p.stream.app.DedupStreamsApplication   : No active profile set, falling back to 1 default profile: "default"
2025-10-23 21:43:29.472  WARN 26832 --- [           main] s.c.a.AnnotationConfigApplicationContext : Exception encountered during context initialization - cancelling refresh attempt: org.springframework.beans.factory.UnsatisfiedDependencyException: Error creating bean with name 'buildDedupStream' defined in com.path.stream.app.DedupStreamsApplication: Unsatisfied dependency expressed through method 'buildDedupStream' parameter 0; nested exception is org.springframework.beans.factory.NoSuchBeanDefinitionException: No qualifying bean of type 'org.apache.kafka.streams.StreamsBuilder' available: expected at least 1 bean which qualifies as autowire candidate. Dependency annotations: {}
2025-10-23 21:43:29.482  INFO 26832 --- [           main] ConditionEvaluationReportLoggingListener : 

Error starting ApplicationContext. To display the conditions report re-run your application with 'debug' enabled.
2025-10-23 21:43:29.508 ERROR 26832 --- [           main] o.s.b.d.LoggingFailureAnalysisReporter   : 

***************************
APPLICATION FAILED TO START
***************************

Description:

Parameter 0 of method buildDedupStream in com.path.stream.app.DedupStreamsApplication required a bean of type 'org.apache.kafka.streams.StreamsBuilder' that could not be found.


Action:

Consider defining a bean of type 'org.apache.kafka.streams.StreamsBuilder' in your configuration. 