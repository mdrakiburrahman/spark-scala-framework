# `Spark-Scala-Framework` on Attractions Recommender
## Overview
This repository presents an approach to creating a complete **Software Product/Reusable ETL Framework** on top of the prototype *Recommendation Pipeline* developed in [INSERT LEGACY ETL CODE LINK](google.ca).<br>

A visual representation is presented below:<br><br>
<img src="https://image.shutterstock.com/image-vector/default-ui-image-placeholder-wireframes-260nw-1037719192.jpg" width="400"><br>

### JAR Compilation
Use `mvn install` for creating a [Fat JAR with Maven](http://tutorials.jenkov.com/maven/maven-build-fat-jar.html) containing all the dependencies compiled. <br>

In case we don't want to include the Spark dependencies in our JAR (as Spark dependencies can/will be managed on the Databricks Clusters as a seperate CICD pipeline), we can simply change the scope of the Spark dependency `spark.scope` in [pom.xml](pom.xml) from `compile` to `provided`, like so: <br><br>
<img src="img/1.png" width="800"><br>

This framework is structured such that setting this variable will override whether or not Spark libraries are managed from the repo (`compile`) or at cluster runtime (`provided`)

### Pipeline Structure

The *Recommender Pipeline* is split into four independent, reusable components:
1. **common -** *Configurations*, *Feeds* management and *Spark Session* management are each isolated in this package, on which all the components depend.
2. **etl -** reads the **raw or bronze** data, transforms it and loads it into the feeds that will then be consumed by other components. Currently loads `visits` and `attractions` from Sigir dataset.
3. **serving -** loads the trained model and exposes a recommendations service. Supports different recommenders to coexist.
4. **training -** reads the `visits` feed and trains the ALS model. Currently we are demo-ing the Pipeline using Spark's [Alternating Least Squares](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.recommendation.ALS) algorithm, though this can be easily expanded to any library by leveraging the presented structure.

Screenshot of each component from the repository:<br>
<img src="img/2.png" width="125"><br>

### Configuration

Some aspects of the different components are driven by **configurations**, which are managed in the common package. The values to be used for these configurable aspects should be set as environment variables when running each component, allowing them to be specific to different run environments.

### Feeds

The common package contains a simple framework for handling feeds, to which the components should delegate their IO.

These feeds are stored in a file system, in a root path that should be provided via configurations. By using this feed framework, different components can run completely independently and easily synchronize using a file system acting as a "whiteboard".

This can include DBFS local file system and mounted storage (e.g. ADLS) to be supported seamlessly - e.g. once the ADLS Filesystem is mounted on a Databricks Workspace, it can be accessed via `/mnt/`. Also, the feeds framework handles the versioning of the different updates in a feed using update timestamps in a transparent way, enhancing the auditability of the system. Of course, leveraging **Delta Lake** would achieve the same purpose, although this is an enhancement we can build into this repository quite easily (currently includes classes for `CSV` and `Parquet` Feeds):<br><br>
<img src="img/3.png" width="175">

### Paneled Recommender

In order to support different recommenders to coexist in the recommendations service, we provide a *paneled* implementation of the recommender that can use different implementations for different panels. This can be used for doing A/B tests.

When a recommendation request comes, the paneled recommender applies a deterministic hashing function on some characteristics of the requests and uses that hash to pick the panel to which the request belongs. <br>

A few things to note:
- It is important that we can re-create the hashing function used when analyzing recommendation logs to attribute behavior to the right implementation.
- The natural pick in this case would be to hash the user id in the request. Even though that's valid and useful, it is recommended to consider adding some "salt" to that so as to not always include the same users as exposed in our experiments. In this case we are using the `minute-of-the-hour` at the time of the request. Of course, we must be able to re-create our salting when re-creating the hashing function for analysis.

### Drivers

A few driver apps are included for the different components. Even though these should be helpful (especially the component-specific drivers that use feeds for synchronizing with other components), one could also compose the units in the code in any way needed.

### Debugging

The common package contains a DataFrame descriptor utility that is only initiated - by design - when `debug` is configured to be enabled:<br>
<img src="img/4.png" width="800"><br><br>

This is *extremely useful*, because:
- We want to use the same code in `DEV`, `TEST` & `PROD` without commenting out lines for verbose logging
- When in `DEV` & `TEST`, we often use smaller datasets and like to use verbose logging for debugging.
- When in `PROD`, we don't want to `display` datasets because:
    - The data may be PII (i.e. `display` would visualize the dataset at runtime)
    - Performing `display` or `describe` on a large dataset can be an extremely expensive **Action** that slows down our Pipeline (with no value add).

This descriptor prints basic stats about the data, and can be extended for any specific cases.

An IDE is also a great tool for debugging, and the way in which the components are organized makes it easy to do interactive debugging of each piece in isolation.
