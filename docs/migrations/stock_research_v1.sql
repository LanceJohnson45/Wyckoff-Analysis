-- Maintained schema v1, generated from stock_research/schema.py.
-- PostgreSQL only. Apply using the authorized research schema owner.
BEGIN;
SET LOCAL TIME ZONE 'Asia/Shanghai';
CREATE SCHEMA IF NOT EXISTS stock_research;

CREATE TABLE IF NOT EXISTS stock_research.classification_versions (
	uuid VARCHAR(36) NOT NULL,
	workspace VARCHAR(128) NOT NULL,
	version VARCHAR(256) NOT NULL,
	content_hash VARCHAR(64) NOT NULL,
	payload JSON NOT NULL,
	create_time TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	update_time TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	delete_time TIMESTAMP WITH TIME ZONE,
	is_deleted BOOLEAN DEFAULT 'false' NOT NULL,
	PRIMARY KEY (uuid),
	UNIQUE (workspace, version)
);

CREATE TABLE IF NOT EXISTS stock_research.universes (
	uuid VARCHAR(36) NOT NULL,
	workspace VARCHAR(128) NOT NULL,
	version VARCHAR(256) NOT NULL,
	content_hash VARCHAR(64) NOT NULL,
	payload JSON NOT NULL,
	create_time TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	update_time TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	delete_time TIMESTAMP WITH TIME ZONE,
	is_deleted BOOLEAN DEFAULT 'false' NOT NULL,
	PRIMARY KEY (uuid),
	UNIQUE (workspace, version)
);

CREATE TABLE IF NOT EXISTS stock_research.group_members (
	uuid VARCHAR(36) NOT NULL,
	classification_uuid VARCHAR(36) NOT NULL,
	symbol VARCHAR(32) NOT NULL,
	kind VARCHAR(32) NOT NULL,
	name VARCHAR(256) NOT NULL,
	effective_date DATE NOT NULL,
	payload JSON NOT NULL,
	create_time TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	update_time TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	delete_time TIMESTAMP WITH TIME ZONE,
	is_deleted BOOLEAN DEFAULT 'false' NOT NULL,
	PRIMARY KEY (uuid),
	UNIQUE (classification_uuid, symbol, kind, name),
	FOREIGN KEY(classification_uuid) REFERENCES stock_research.classification_versions (uuid)
);

CREATE TABLE IF NOT EXISTS stock_research.market_snapshots (
	uuid VARCHAR(36) NOT NULL,
	workspace VARCHAR(128) NOT NULL,
	market VARCHAR(8) NOT NULL,
	as_of DATE NOT NULL,
	schema_version INTEGER NOT NULL,
	universe_uuid VARCHAR(36) NOT NULL,
	classification_uuid VARCHAR(36) NOT NULL,
	content_hash VARCHAR(64) NOT NULL,
	payload JSON NOT NULL,
	create_time TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	update_time TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	delete_time TIMESTAMP WITH TIME ZONE,
	is_deleted BOOLEAN DEFAULT 'false' NOT NULL,
	PRIMARY KEY (uuid),
	UNIQUE (workspace, content_hash),
	FOREIGN KEY(universe_uuid) REFERENCES stock_research.universes (uuid),
	FOREIGN KEY(classification_uuid) REFERENCES stock_research.classification_versions (uuid)
);

CREATE TABLE IF NOT EXISTS stock_research.universe_members (
	uuid VARCHAR(36) NOT NULL,
	universe_uuid VARCHAR(36) NOT NULL,
	symbol VARCHAR(32) NOT NULL,
	index VARCHAR(32) NOT NULL,
	effective_date DATE NOT NULL,
	expiry_date DATE,
	payload JSON NOT NULL,
	create_time TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	update_time TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	delete_time TIMESTAMP WITH TIME ZONE,
	is_deleted BOOLEAN DEFAULT 'false' NOT NULL,
	PRIMARY KEY (uuid),
	UNIQUE (universe_uuid, symbol, index, effective_date),
	FOREIGN KEY(universe_uuid) REFERENCES stock_research.universes (uuid)
);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_workspace_as_of ON stock_research.market_snapshots (workspace, as_of);
COMMIT;
