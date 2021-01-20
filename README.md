# Programmatic Guaranteed General Planner

The General Planner component is responsible for
- Periodically retrieving line item metadata and delivery plans from vendor Planner Adapters
- Performing frequent host based adjustments on delivery plans based on PBS delivery feedback
- Serving line item metadata and adjusted delivery plans to hosted PBS instances on demand

# Getting Started

## Technical stack
- CentOS 7.3
- Java 8
- Vert.x 
- Spring Boot
- MySQL
- Lombok
- JUnit5, Mockito, H2
- Maven

The server responds to several HTTP [server endpoints](docs/server_endpoints.md) 
and gets data from several HTTP  several HTTP [remote endpoints](docs/remote_endpoints.md).


## Building

To build the project, you will need 
[Java 8 JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
and [Maven](https://maven.apache.org/) installed.

To verify the installed Java run in console:
```bash
java -version
```
which should show something like (yours may be different but must show 1.8):
```
java version "1.8.0_241"
Java(TM) SE Runtime Environment (build 1.8.0_241-b07)
Java HotSpot(TM) 64-Bit Server VM (build 25.241-b07, mixed mode)
```

Follow next steps to create JAR which can be deployed locally. 
- Download or clone a project and checkout master or master-dev (once open sourced this information will change)
```bash
git clone https://github.rp-core.com/ContainerTag/pg-general-planner.git
```

- Move to project directory:
```bash
cd pg-general-planner
```

- Run below command to build project:
```bash
mvn clean package
```


## Configuration

Configuration is handled by [Spring Boot](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html), 
which supports properties files, YAML files, environment variables and command-line arguments for setting config values.

The server requires a MySQL database version 5.7.x (x being 23 and up) to be available with the [gp schema](sql/pg-gp-init-db.sql) created.

The source code includes minimal required configuration file `src/main/resources/application.yaml`.
These properties can be extended or modified with an external configuration file.

For example, `gp-config.yaml`:
```yaml
metrics:
  graphite:
    enabled: true
    prefix: custom.pg-central.gp
    host: localhost
    port: 3003
    interval: 60
```
For properties not specified or overriden in `gp-config.yaml`, application will look for default settings  in `src/main/resources/application.yaml` file.

To use external application configuration just add the following as start up arguments:
```bash
--spring.config.additional-location=/path/to/gp-config.yaml
```

Details on all available configuration tags can be found in [configuration document](docs/config-app.md)

## Running

The project build has been tested at runtime with a Java 8 runtime. 
Run your local server (you may need to point to active remote URLs and a valid database schema to avoid seeing errors in the logs) with a command similar to:
```bash
java -jar target/pg-general-planner.jar --spring.config.additional-location=sample/gp-config.yaml 
```

## Basic check

Go to [http://localhost:8080/general-planner/api/v1/alive](http://localhost:8080/general-planner/api/v1/alive) 
and verify that response status is `200`.


## Code Style

The [pom.xml](pom.xml) is configured to enforce a coding style defined in [checkstyle.xml](checkstyle.xml).

The intent here is to maintain a common style across the project and rely on the process to enforce it instead of individuals.