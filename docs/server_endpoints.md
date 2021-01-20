# Server Endpoints 

General Planner hosts several endpoints, and all server endpoints are protected with HTTP Basic Authentication.

## PBS Registration Endpoint

Allows each PBS instance to register itself with the General Planner.

### `POST /general-planner/api/v1/register` 

This endpoint is used to notify about event and do request for tracking pixel if needed.

#### Request payload

A sample request payload can be found [here](samples/pbs_registration_request.json).

### Expected Response

Empty response with HTTP Status 200.

###Expected Response with admin directives

Sometimes the response will contain admin directives for the PBS instance to execute, as listed in the samples below:
- [tracer](samples/pbs_registration_response_with_tracer_admin.json)
- [suspend services](samples/pbs_registration_response_with_suspend_admin.json)
- [refresh stored requests cache](samples/pbs_registration_request_with_save_stored_request_admin.json)
- [invalidate stored requests cache](samples/pbs_registration_request_with_invalidate_stored_request_admin.json)
- [refresh amp stored requests cache](samples/pbs_registration_request_with_save_stored_request_amp_admin.json)
- [invalidate amp stored requests cache](samples/pbs_registration_request_with_invalidate_stored_request_amp_admin.json)


## Get Plans Endpoint

Allows each PBS instance to retrieve line items and plans from the General Planner.

### `GET /general-planner/api/v1/plans`

#### Query parameters

| Parameter | Format | Required? | Description |
| --- | --- | --- | --- |
| vendor | string | yes | PBS hosting vendor |
| region | string | yes | Data center region for which the plan is requested for |
| instanceId | string | yes | The requesting PBS instance/host identifier |

#### Expected Response

General Planner will respond with line items and plans for the PBS as in the [get plans response sample](samples/pbs_get_plans_response.json).


## PBS Status API

PBS reports its status via registration requests. Such global PBS status is accessible via this API.

#### `GET /general-planner/api/v1/pbs-health`

#### Query parameters

| Parameter | Format | Required? | Description |
| --- | --- | --- | --- |
| vendor | string | no | PBS hosting vendor |
| region | string | no | Data center region for the PBS |
| instanceId | string | no | PBS instance/host identifier |

#### Expected Response

When instanceId is specified, the General Planner will reply as in [pbs health response sample](samples/pbs_health_with_instance_id_response.json).


## Suspend Services Admin API

This endpoint receives an administrative command to suspend the General Planner API and its services.

### `POST /general-planner/api/v1/prep-for-shutdown`

#### Expected Response

Empty response with HTTP Status 200.


## Resume Services Admin API

### `POST /general-planner/api/v1/cease-shutdown`

This endpoint receives an administrative command to resume the General Planner API and its services.

#### Expected Response

Empty response with HTTP Status 200.


## Admin API for other tasks

This could have been combined with the above shutdown/resume APIs into one API

### `POST /general-planner/api/v1/admin`

### Request body

Can perform the following: 
- [turn on General Planner Trace](samples/admin_gp_turn_on_trace.json)
- [turn on PBS trace](samples/admin_gp_turn_on_pbs_trace.json)
- [suspend PBS services](samples/admin_gp_suspend_pbs_services.json)
- [refresh stored request cache](samples/admin_gp_refresh_stored_request_in_pbs.json)
- [refresh amp stored request cache](samples/admin_gp_refresh_stored_request_amp_in_pbs.json)
- [invalidate stored request cache](samples/admin_gp_invalidate_stored_request_in_pbs.json)
- [invalidate amp stored request cache](samples/admin_gp_invalidate_stored_request_amp_in_pbs.json)
