# Remote endpoints

General communicates with several remote endpoints

## Planner Adapter Endpoint

The General Planner retrieves line item and plan metadata from one or more vendor hosted service, typically referred to as the Planner Adapter. 
The General Planner supports HTTP Basic Auth in this interaction.
This Planner Adapter endpoint must adhere to the following specifications:

#### HTTP Action

`GET`

#### Query Parameter

| Parameter | Format | Required? | Description |
| --- | --- | --- | --- |
| since | string | no |  Timestamp in ISO-8601 format. For example, 2019-02-01T03:00:00.000Z. Service should respond with all meta data for active or nearly-active line items and schedules that got updated since this timestamp. Absence of this parameter signals request to return all active or nearly-active line items. |
| hours | string | no |  Number of hours of plans desired i.e. provide the next 3 hours worth of plans |

#### Sample Response

As available [here](samples/pa_rsp.json)


## Delivery Stats Service Endpoint

The General Planner retrieves line item target matched feedback data from the Delivery Stats Service (which aggregates the data as received from PBS instances). 
The General Planner uses HTTP Basic Auth in this interaction.
This Delivery Stats Service endpoint must adhere to the following specifications:

#### HTTP Action

`GET`

#### Query Parameter

| Parameter | Format | Required? | Description |
| --- | --- | --- | --- |
| since | string | no |  Timestamp in ISO-8601 format. For example, 2019-02-01T03:00:00.000Z. 
| vendor | string | no |  PBS vendor
| region | string | no | Region in which PBS is deployed, like us-west-2

If specified, both vendor and region must be present.

#### Sample Response
As available [here](samples/del_stats_to_gp_rsp.json)

Note that the fields tokenSpent and serviceInstanceId are not useful in General Planner processing.
