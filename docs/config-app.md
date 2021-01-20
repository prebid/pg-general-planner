
# Full list of application configuration options

This document describes all the configuration properties available for the PG General Planner.

## Vert.x
- `vertx.worker-pool-size` -  maximum number of worker threads to be used by the Vert.x instance
- `vertx.http-server-instances` - number of http server instances to create

## HTTP
- `http.port` - HTTP listener port
- `idle-timeout-sec` - as in idleTimeout described [here](https://vertx.io/docs/apidocs/io/vertx/core/http/HttpServerOptions.html#setIdleTimeout-int-)
- `base-url` - base service request mapping URL path
- `admin-base-url` - base service request mapping URL path for admin services

## Deployment
- `deployment.profile` - could be `dev`, `test`, `prod`, or any other deployment environment
- `deployment.infra` - if set to `ecs`, system can be deployed on a managed single instance service task in AWS Use 'vm' if deployed on bare metal/virtual machine/EC2 instance.
- `deployment.data-center` - identifier to distinguish among AWS, on-premise or any other data center provider
- `deployment.region` - identifier for data center region
- `deployment.system` - overall system identifier `PG`
- `deployment.sub-system` - sub-system identifier `General Planner` in `PG` system

## Database
- `database.general-planner.dbname` - schema name in MySQL 
- `database.general-planner.user` - application user for above schema  
- `database.general-planner.password` - password for application user - should be supplied externally in protected fashion 
- `database.general-planner.host` - database hostname 
- `database.general-planner.port` - database port 
- `database.general-planner.initial-pool-size` - initial size of database connection pool
- `database.general-planner.min-pool-size` - minimum size of database connection pool
- `database.general-planner.max-pool-size` - maximum size of database connection pool
- `database.general-planner.max-idle-time-sec` - idle time in seconds before session is terminated  
- `database.circuit-breaker.opening-threshold` - the number of failures before opening the circuit
- `database.circuit-breaker.closing-interval-sec` - time spent in open state before attempting to re-try

## HTTP Client
- `http-client.max-pool-size` - the maximum pool size for outgoing HTTP connections
- `http-client.connect-timeout-sec` - the connect timeout value
- `http-client.circuit-breaker.opening-threshold` - the number of failures before opening the circuit
- `http-client.circuit-breaker.closing-interval-sec` - time spent in open state before attempting to re-try

## Delivery Data Services
- `services.delivery-data.enabled` - true/false boolean flag to enable this service
- `services.delivery-data.username` - HTTP Basic Auth user to access Delivery Stats Service
- `services.delivery-data.password` - HTTP Basic Auth password to access Delivery Stats Service
- `services.delivery-data.initial-delay-sec` - delay in seconds after system restart to start the service
- `services.delivery-data.refresh-period-sec` - gap in seconds before each delivery data refresh
- `services.delivery-data.url` - URL to GET gap in seconds between each service run

## Host Reallocation Services
- `services.host-reallocation.enabled` - boolean flag to enable this service
- `services.host-reallocation.initial-delay-sec` - delay in seconds after system restart to start the service
- `services.host-reallocation.refresh-period-sec` - gap in seconds between each service run
- `services.host-reallocation.db-store-batch-size` - number of rows to store in the `reallocated_plans` table in a batch
- `services.host-reallocation.reallocation-updated-since-min` - overwrite `hots` plans for hosts that are active since specified minutes
- `services.host-reallocation.line-item-has-expired-min` - include in reallocation lines expired in the specified number of minutes
- `services.host-reallocation.algorithm` - class name of reallocation algorithm. New implementations can be plugged in.
- `services.host-reallocation.algorithm.algorithm-spec.non-adjustable-share-percent` - percentage of reallocation that cannot be adjusted per reallocation

## Planner Adapter Services
- `services.planner-adapters.planners[0].name` - include in reallocation lines expired in the specified number of minutes
- `services.planner-adapters.planners[0].enabled` - boolean flag to enable this service
- `services.planner-adapters.planners[0].url` - the Planner Adapter URL
- `services.planner-adapters.planners[0].username` - HTTP Basic Auth user to access this Planner Adapter
- `services.planner-adapters.planners[0].password` - HTTP Basic Auth password to access to access this Planner Adapter
- `services.planner-adapters.planners[0].initial-delay-sec` - delay in seconds after system restart to start the service
- `services.planner-adapters.planners[0].refresh-period-sec` - gap in seconds between each service run
- `services.planner-adapters.planners[0].timeout-sec` - timeout in seconds in call to Planner Adapter URL
- `services.planner-adapters.planners[0].future-plan-hours` - include in reallocation lines expired in the specified number of minutes
- `services.planner-adapters.planners[0].bidder-code-prefix` - a prefix to associate bidder with PG deals

## Alert Proxy 
- `services.alert-proxy.enabled` - boolean flag to enable this service
- `services.alert-proxy.url` - the Alert Proxy URL
- `services.alert-proxy.timeout-sec` - timeout in seconds in call to Alert Proxy URL
- `services.alert-proxy.url` - the Alert Proxy URL
- `services.alert-proxy.username` - HTTP Basic Auth user to access the Alert Proxy
- `services.alert-proxy.password` - HTTP Basic Auth password to access the Alert Proxy
- `services.alert-proxy.policies[0].alert-name` - default alert throttle policy
- `services.alert-proxy.policies[0].initial-alerts` - the number of alerts to send without throttling
- `services.alert-proxy.policies[0].alert-frequency` - alert throttling level. Send Nth alerts.

## Server Authentication
- `server-auth.authentication-enabled` - boolean flag to enable authentication
- `server-auth.principals[0].username` - username
- `server-auth.principals[0].password` - password
- `server-auth.principals[0].roles` - comma separated roles assigned to this user
- `server-api-roles.registration` - role allowing PBS registration actions
- `server-api-roles.plan-request` - role allowing PBS plan request actions
- `server-api-roles.pbs-health` - read only role allowing pbs-health actions
- `server-api-roles.admin` - role allowing all actions

## Metrics
- `metrics.graphite.enabled` - boolean flag to enable publishing metrics to Graphite
- `metrics.graphite.prefix` - prefix to classify metrics source
- `metrics.graphite.host` - target graphite host
- `metrics.graphite.port` - target graphite port
- `metrics.graphite.interval` - interval in seconds to publish metrics

## Admin Interface
- `admin.apps` - comma separated list of applications recognizing admin events
- `admin.db-store-batch-size` - batch size to store admin commands in database
- `admin.trace.max-duration-in-seconds` - maximum duration in seconds for which targeted trace can be turned on
 
## General settings
- `error.message` - canned message for backend errors
- `services.pbs-max-idle-period-sec` - maximum duration in seconds of no registration received for a PBS instance before it is deemed inactive
