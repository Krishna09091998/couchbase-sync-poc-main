2025-10-23 21:47:41.690  INFO 6364 --- [           main] c.p.stream.app.DedupStreamsApplication   : Starting DedupStreamsApplication using Java 1.8.0_302 on 01HW2133554 with PID 6364 (C:\Temp\MT_REPOS\path-kafka-internal-custom-connectors\path-kafka-streams\target\classes started by 2921892 in C:\Temp\MT_REPOS\path-kafka-internal-custom-connectors\path-kafka-streams)
2025-10-23 21:47:41.694  INFO 6364 --- [           main] c.p.stream.app.DedupStreamsApplication   : No active profile set, falling back to 1 default profile: "default"
2025-10-23 21:47:42.209  WARN 6364 --- [           main] s.c.a.AnnotationConfigApplicationContext : Exception encountered during context initialization - cancelling refresh attempt: org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'buildDedupStream' defined in com.path.stream.app.DedupStreamsApplication: Bean instantiation via factory method failed; nested exception is org.springframework.beans.BeanInstantiationException: Failed to instantiate [org.apache.kafka.streams.kstream.KStream]: Factory method 'buildDedupStream' threw exception; nested exception is java.lang.RuntimeException: Failed to load mapping file
2025-10-23 21:47:42.220  INFO 6364 --- [           main] ConditionEvaluationReportLoggingListener : 

Error starting ApplicationContext. To display the conditions report re-run your application with 'debug' enabled.
2025-10-23 21:47:42.254 ERROR 6364 --- [           main] o.s.boot.SpringApplication               : Application run failed

org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'buildDedupStream' defined in com.path.stream.app.DedupStreamsApplication: Bean instantiation via factory method failed; nested exception is org.springframework.beans.BeanInstantiationException: Failed to instantiate [org.apache.kafka.streams.kstream.KStream]: Factory method 'buildDedupStream' threw exception; nested exception is java.lang.RuntimeException: Failed to load mapping file
        at org.springframework.beans.factory.support.ConstructorResolver.instantiate(ConstructorResolver.java:658) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.beans.factory.support.ConstructorResolver.instantiateUsingFactoryMethod(ConstructorResolver.java:638) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.instantiateUsingFactoryMethod(AbstractAutowireCapableBeanFactory.java:1352) ~[spring-beans-5.3.24.jar:5.3.24]     
        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBeanInstance(AbstractAutowireCapableBeanFactory.java:1195) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:582) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:542) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.beans.factory.support.AbstractBeanFactory.lambda$doGetBean$0(AbstractBeanFactory.java:335) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:234) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:333) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:208) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingletons(DefaultListableBeanFactory.java:955) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.context.support.AbstractApplicationContext.finishBeanFactoryInitialization(AbstractApplicationContext.java:918) ~[spring-context-5.3.24.jar:5.3.24]
        at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:583) ~[spring-context-5.3.24.jar:5.3.24]
        at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:732) [spring-boot-2.7.18.jar:2.7.18]
        at org.springframework.boot.SpringApplication.refreshContext(SpringApplication.java:409) [spring-boot-2.7.18.jar:2.7.18]
        at org.springframework.boot.SpringApplication.run(SpringApplication.java:308) [spring-boot-2.7.18.jar:2.7.18]
        at org.springframework.boot.SpringApplication.run(SpringApplication.java:1300) [spring-boot-2.7.18.jar:2.7.18]
        at org.springframework.boot.SpringApplication.run(SpringApplication.java:1289) [spring-boot-2.7.18.jar:2.7.18]
        at com.path.stream.app.DedupStreamsApplication.main(DedupStreamsApplication.java:30) [classes/:na]
Caused by: org.springframework.beans.BeanInstantiationException: Failed to instantiate [org.apache.kafka.streams.kstream.KStream]: Factory method 'buildDedupStream' threw exception; nested exception is java.lang.RuntimeException: Failed to load mapping file
        at org.springframework.beans.factory.support.SimpleInstantiationStrategy.instantiate(SimpleInstantiationStrategy.java:185) ~[spring-beans-5.3.24.jar:5.3.24]
        at org.springframework.beans.factory.support.ConstructorResolver.instantiate(ConstructorResolver.java:653) ~[spring-beans-5.3.24.jar:5.3.24]
        ... 18 common frames omitted
Caused by: java.lang.RuntimeException: Failed to load mapping file
        at com.path.stream.app.DedupTopicMapper.<init>(DedupTopicMapper.java:14) ~[classes/:na]      
        at com.path.stream.app.DedupStreamsApplication.buildDedupStream(DedupStreamsApplication.java:56) [classes/:na]
        at com.path.stream.app.DedupStreamsApplication$$EnhancerBySpringCGLIB$$e8c5bf0a.CGLIB$buildDedupStream$2(<generated>) ~[classes/:na]
        at com.path.stream.app.DedupStreamsApplication$$EnhancerBySpringCGLIB$$e8c5bf0a$$FastClassBySpringCGLIB$$fa623117.invoke(<generated>) ~[classes/:na]
        at org.springframework.cglib.proxy.MethodProxy.invokeSuper(MethodProxy.java:244) ~[spring-core-5.3.31.jar:5.3.31]
        at org.springframework.context.annotation.ConfigurationClassEnhancer$BeanMethodInterceptor.intercept(ConfigurationClassEnhancer.java:331) ~[spring-context-5.3.24.jar:5.3.24]
        at com.path.stream.app.DedupStreamsApplication$$EnhancerBySpringCGLIB$$e8c5bf0a.buildDedupStream(<generated>) ~[classes/:na]
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:1.8.0_302]
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[na:1.8.0_302]
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:1.8.0_302]
        at java.lang.reflect.Method.invoke(Method.java:498) ~[na:1.8.0_302]
        at org.springframework.beans.factory.support.SimpleInstantiationStrategy.instantiate(SimpleInstantiationStrategy.java:154) ~[spring-beans-5.3.24.jar:5.3.24]
        ... 19 common frames omitted
Caused by: java.io.FileNotFoundException: dedup-mapping.properties (The system cannot find the file specified)
        at java.io.FileInputStream.open0(Native Method) ~[na:1.8.0_302]
        at java.io.FileInputStream.open(FileInputStream.java:195) ~[na:1.8.0_302]
        at java.io.FileInputStream.<init>(FileInputStream.java:138) ~[na:1.8.0_302]
        at java.io.FileInputStream.<init>(FileInputStream.java:93) ~[na:1.8.0_302]
        at com.path.stream.app.DedupTopicMapper.<init>(DedupTopicMapper.java:11) ~[classes/:na]      
        ... 30 common frames omitted