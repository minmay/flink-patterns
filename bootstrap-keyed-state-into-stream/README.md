# Bootstrap Keyed State into a Stream

According to the documentation, and various blogs, it is possible to use the Batch Execution Environment
to bootstrap state into a save point, and then load that state in a Stream Execution Environment.

The goal of this pattern is to document a working example of how to achieve that.

[State Processor API](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/libs/state_processor_api.html) 
documentation states that "you can read a batch of data from any store, preprocess it, and write the result to a
 savepoint that you use to bootstrap the state of a streaming application."

Kartik Khare in his [blog](https://www.kharekartik.dev/2019/12/14/bootstrap-your-flink-jobs/) wrote that 
"You can create both Batch and Stream environment in a single job."

However, I have yet to find a working example that shows how to do both. I wrote a solution.

## Run with Docker Compose

1. ```git clone https://github.com/minmay/flink-patterns.git```
2. ```cd flink-patterns/bootstrap-keyed-state-into-stream```
3. ```docker-compose up```

## Solution Notes

As of August 10, 2020, Amazon EMR only supports Flink 1.10.1 and JDK 8. It was imperative that my solution 
works with that environment.

Initially, I tried to solve this problem locally, but I but I believe that there is a bug in saving Keyed State
within the ```KeyedStateBootstrapFunction``` when run locally (ie in an IDE). Thus, the only way
I got this to work was by deploying to an actual cluster. My code shows how to deploy and submit jobs
to a cluster within docker-compose. I customized the Flink Docker image to use JDK 8 (instead of JRE) so 
that I could use the Flink command-line tool within my Docker images (if anybody knows of a better
way of doing this, please let me know).

1. run the "write bootstrap" batch job in Flink that programmatically writes a save point. In my code, that is that
that path use the ```void bootstrap()``` method.
2. after, run the "read bootstrap" stream job with the -s parameter to load the savepoint that was written by the
"write bootstrap" job (please note, I think none of the documentation, nor the blogs were explicit in detailing
to a Flink beginner that the load save point has to be explicitly declared in the start up command line parameters
when submitting the job. I hope to find a way to programmatically load a save point).
3. I opened a volume in Docker that is shared by jobmanager, taskmanager, and Flink job. My Flink job
run script created this directory, and this is where it saves and reads the save point.

If you run this repetitively, you might want to clear the save points by removing the volumn.
```docker-compose down -v``` will remove the volume.   
