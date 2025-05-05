CREATE TABLE `vigrt_tenant` (
  `tenant_id` text UNIQUE PRIMARY KEY NOT NULL,
  `tenant_name` text UNIQUE NOT NULL,
  `display_name` text
);

CREATE TABLE `vigrt_site_collections` (
  `site_collection_id` text UNIQUE PRIMARY KEY NOT NULL,
  `tenant_id` text NOT NULL,
  `root_site_id` text NOT NULL,
  `name` text,
  `url` text,
  `created_at` timestamp,
  `modified_at` timestamp,
  `site_template` text,
  `has_unique_roles` boolean,
  `change_token` text
);

CREATE TABLE `vigrt_subsites` (
  `subsite_id` text UNIQUE PRIMARY KEY NOT NULL,
  `tenant_id` text NOT NULL,
  `site_collection_id` text NOT NULL,
  `parent_site_id` text NOT NULL,
  `site_template` text,
  `url` text,
  `created_at` timestamp,
  `allow_rss` boolean,
  `last_item_mod_at` timestamp,
  `last_item_user_mod_at` timestamp,
  `admin_share_enabled` boolean,
  `change_token` text,
  `hierarchy_level` integer
);

CREATE TABLE `vigrt_lists` (
  `list_id` text UNIQUE PRIMARY KEY NOT NULL,
  `tenant_id` text NOT NULL,
  `site_collection_id` text NOT NULL,
  `parent_site_id` text NOT NULL,
  `title` text,
  `created_at` timestamp,
  `modified_at` timestamp,
  `change_token` text,
  `hierarchy_level` integer
);

CREATE TABLE `vigrt_member` (
  `principal_id` text UNIQUE PRIMARY KEY NOT NULL,
  `tenant_id` text NOT NULL,
  `login_name` text,
  `display_name` text,
  `email` text,
  `principal_type` text
);

CREATE TABLE `vigrt_role_definition` (
  `role_definition_id` text UNIQUE PRIMARY KEY NOT NULL,
  `tenant_id` text NOT NULL,
  `name` text,
  `description` text
);

CREATE TABLE `vigrt_resource` (
  `resource_id` text UNIQUE PRIMARY KEY NOT NULL,
  `tenant_id` text NOT NULL,
  `site_collection_id` text NOT NULL,
  `type` text,
  `hierarchy_level` integer,
  `ingested_at` timestamp
);

CREATE TABLE `vigrt_role_assignment` (
  `assignment_id` text UNIQUE PRIMARY KEY NOT NULL,
  `tenant_id` text NOT NULL,
  `site_collection_id` text NOT NULL,
  `resource_id` text NOT NULL,
  `principal_id` text NOT NULL,
  `role_definition_id` text NOT NULL,
  `change_token` text
);

ALTER TABLE `vigrt_site_collections` ADD FOREIGN KEY (`tenant_id`) REFERENCES `vigrt_tenant` (`tenant_id`);

ALTER TABLE `vigrt_subsites` ADD FOREIGN KEY (`tenant_id`) REFERENCES `vigrt_tenant` (`tenant_id`);

ALTER TABLE `vigrt_subsites` ADD FOREIGN KEY (`site_collection_id`) REFERENCES `vigrt_site_collections` (`site_collection_id`);

ALTER TABLE `vigrt_lists` ADD FOREIGN KEY (`tenant_id`) REFERENCES `vigrt_tenant` (`tenant_id`);

ALTER TABLE `vigrt_lists` ADD FOREIGN KEY (`site_collection_id`) REFERENCES `vigrt_site_collections` (`site_collection_id`);

ALTER TABLE `vigrt_member` ADD FOREIGN KEY (`tenant_id`) REFERENCES `vigrt_tenant` (`tenant_id`);

ALTER TABLE `vigrt_role_definition` ADD FOREIGN KEY (`tenant_id`) REFERENCES `vigrt_tenant` (`tenant_id`);

ALTER TABLE `vigrt_resource` ADD FOREIGN KEY (`tenant_id`) REFERENCES `vigrt_tenant` (`tenant_id`);

ALTER TABLE `vigrt_resource` ADD FOREIGN KEY (`site_collection_id`) REFERENCES `vigrt_site_collections` (`site_collection_id`);

ALTER TABLE `vigrt_role_assignment` ADD FOREIGN KEY (`tenant_id`) REFERENCES `vigrt_tenant` (`tenant_id`);

ALTER TABLE `vigrt_role_assignment` ADD FOREIGN KEY (`site_collection_id`) REFERENCES `vigrt_site_collections` (`site_collection_id`);

ALTER TABLE `vigrt_role_assignment` ADD FOREIGN KEY (`resource_id`) REFERENCES `vigrt_resource` (`resource_id`);

ALTER TABLE `vigrt_role_assignment` ADD FOREIGN KEY (`principal_id`) REFERENCES `vigrt_member` (`principal_id`);

ALTER TABLE `vigrt_role_assignment` ADD FOREIGN KEY (`role_definition_id`) REFERENCES `vigrt_role_definition` (`role_definition_id`);
