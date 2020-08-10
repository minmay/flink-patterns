# Bootstrap Keyed State into a Stream

According to the documentation, and various blogs, it is possible to use the Batch Execution Environment
to bootstrap state into a save point, and then load that state in a Stream Execution Environment.

The goal of this pattern is to document a working example of how to achieve that.

[State Processor API](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/libs/state_processor_api.html) 
documentation states that "you can read a batch of data from any store, preprocess it, and write the result to a
 savepoint that you use to bootstrap the state of a streaming application."

Kartik Khare in his [blog](https://www.kharekartik.dev/2019/12/14/bootstrap-your-flink-jobs/) wrote that 
"You can create both Batch and Stream environment in a single job."

However, I have yet to find a working example that shows how to do both.

## Run with Docker Compose

1. ```git clone https://github.com/minmay/flink-patterns.git```
2. ```cd flink-patterns```
3. ```docker-compose up```

Please note that this approach works.

## Run with JDK 11

1. ```git clone https://github.com/minmay/flink-patterns.git```
2. ```cd flink-patterns```
3. ```./gradlew  :bootstrap-keyed-state-into-stream:run```

Please note that this approach fails.