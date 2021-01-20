CREATE DATABASE IF NOT EXISTS gp;

GRANT ALL PRIVILEGES ON gp.* TO 'gp'@'%' IDENTIFIED BY 'gp';
GRANT ALL PRIVILEGES ON gp.* TO 'gp'@'localhost' IDENTIFIED BY 'gp';

USE gp;

-- Create syntax for TABLE 'app_registration'
CREATE TABLE `app_registration` (
  `app_name` varchar(64) NOT NULL,
  `vendor` varchar(64) NOT NULL,
  `region` varchar(32) NOT NULL,
  `instance_id` varchar(64) NOT NULL,
  `health_index` decimal(2,1) NOT NULL,
  `ad_reqs_per_sec` mediumint(9) DEFAULT NULL,
`health_details` json DEFAULT NULL,
`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (`app_name`,`vendor`,`region`,`instance_id`),
KEY `created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'app_registration_history'
CREATE TABLE `app_registration_history` (
`audit_id` int(11) NOT NULL AUTO_INCREMENT,
`app_name` varchar(64) NOT NULL,
`vendor` varchar(64) NOT NULL,
`region` varchar(32) NOT NULL,
`instance_id` varchar(64) NOT NULL,
`health_index` decimal(2,1) NOT NULL,
`ad_reqs_per_sec` mediumint(9) DEFAULT NULL,
`health_details` json DEFAULT NULL,
`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`audit_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`audit_id`),
KEY `audit_time` (`audit_time`)
) ENGINE=InnoDB AUTO_INCREMENT=2314 DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'delivery_token_spend_summary'
CREATE TABLE `delivery_token_spend_summary` (
  `vendor` varchar(32) NOT NULL,
  `region` varchar(32) NOT NULL,
  `instance_id` varchar(128) NOT NULL,
  `line_item_id` varchar(75) NOT NULL,
  `ext_line_item_id` varchar(36) NOT NULL,
  `data_window_start_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `data_window_end_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `report_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `service_instance_id` varchar(128) NOT NULL,
  `summary_data` json NOT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`vendor`,`region`,`instance_id`,`line_item_id`,`service_instance_id`,`data_window_start_timestamp`,`data_window_end_timestamp`),
KEY `report_timestamp` (`report_timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'line_items'
CREATE TABLE `line_items` (
  `general_planner_host_instance_id` varchar(64) NOT NULL,
  `line_item_id` varchar(64) NOT NULL,
  `bidder_code` varchar(32) NOT NULL,
  `status` varchar(16) NOT NULL,
  `line_item_start_date_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `line_item_end_date_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `line_item` json NOT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`general_planner_host_instance_id`,`line_item_id`,`bidder_code`),
KEY `status` (`status`),
KEY `line_item_end_date` (`line_item_end_date_timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'reallocated_plans'
CREATE TABLE `reallocated_plans` (
  `service_instance_id` varchar(64) NOT NULL,
  `vendor` varchar(64) NOT NULL,
  `region` varchar(32) NOT NULL,
  `instance_id` varchar(64) NOT NULL,
  `token_reallocation_weights` json NOT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`service_instance_id`,`vendor`,`region`,`instance_id`),
KEY `updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'reallocated_plans_history'
CREATE TABLE `reallocated_plans_history` (
`audit_id` int(11) NOT NULL AUTO_INCREMENT,
`service_instance_id` varchar(64) NOT NULL,
`vendor` varchar(64) NOT NULL,
`region` varchar(32) NOT NULL,
`instance_id` varchar(64) NOT NULL,
`token_reallocation_weights` json NOT NULL,
`updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
`audit_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`audit_id`),
KEY `audit_time` (`audit_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `line_items_history` (
`audit_id` int(11) NOT NULL AUTO_INCREMENT,
`general_planner_host_instance_id` varchar(64) NOT NULL,
`line_item_id` varchar(64) NOT NULL,
`bidder_code` varchar(32) NOT NULL,
`status` varchar(16) NOT NULL,
`line_item_start_date_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`line_item_end_date_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`line_item` json NOT NULL,
`updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`audit_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`audit_id`),
KEY `audit_time` (`audit_time`),
KEY `updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Create syntax for TABLE 'system_state'
CREATE TABLE `system_state` (
  `tag` varchar(64) NOT NULL,
  `val` varchar(1024) NOT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `line_items_tokens_summary` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`summary_window_start_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`summary_window_end_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`line_item_id` varchar(64) NOT NULL,
`bidder_code` varchar(64) NOT NULL,
`ext_line_item_id` varchar(64) NOT NULL,
`tokens` int(11) NOT NULL DEFAULT '0',
`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (`id`),
KEY `summary_window` (`summary_window_start_timestamp`,`summary_window_end_timestamp`),
KEY `report_timestamp` (`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=201 DEFAULT CHARSET=utf8;

CREATE TABLE `admin_event` (
  `id` varchar(36) NOT NULL,
  `app_name` varchar(64) NOT NULL,
  `vendor` varchar(64) NOT NULL,
  `region` varchar(32) NOT NULL,
  `instance_id` varchar(64) NOT NULL,
  `directive` json NOT NULL,
  `expiry_at` timestamp NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
KEY `app_name` (`app_name`,`vendor`,`region`,`instance_id`),
KEY `created_at` (`created_at`),
KEY `expiry_at` (`expiry_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE  TRIGGER trigger_reallocated_plans BEFORE DELETE ON reallocated_plans
FOR EACH ROW INSERT INTO reallocated_plans_history(
`service_instance_id`, `vendor`, `region`, `instance_id`, `token_reallocation_weights`, `updated_at`)
VALUES (OLD.service_instance_id, OLD.vendor, OLD.region, OLD.instance_id, OLD.token_reallocation_weights, OLD.updated_at);

CREATE TRIGGER `trigger_app_registration` BEFORE DELETE ON `app_registration` FOR EACH ROW INSERT INTO app_registration_history(
`app_name`, `vendor`, `region`, `instance_id`, `health_index`, `ad_reqs_per_sec`, `health_details`, `created_at`)
VALUES (OLD.app_name, OLD.vendor, OLD.region, OLD.instance_id, OLD.health_index, OLD.ad_reqs_per_sec, OLD.health_details, OLD.created_at);

CREATE TRIGGER `trigger_line_items` BEFORE DELETE ON `line_items` FOR EACH ROW INSERT INTO line_items_history(
`general_planner_host_instance_id`, `line_item_id`, `bidder_code`, `status`, `line_item_start_date_timestamp`, `line_item_end_date_timestamp`, `line_item`, `updated_at`)
VALUES (OLD.general_planner_host_instance_id, OLD.line_item_id, OLD.bidder_code, OLD.status, OLD.line_item_start_date_timestamp, OLD.line_item_end_date_timestamp, OLD.line_item, OLD.updated_at);
