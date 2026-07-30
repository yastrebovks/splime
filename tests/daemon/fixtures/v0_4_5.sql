-- Release-faithful local daemon baseline generated with immutable spl v0.4.5.
-- Source tag v0.4.5, commit 4a4231e959ec35776c2c874cf4fbb75c7b8864ae.
-- All rows and paths are synthetic; server connections and stored secrets are absent.
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE schema_migrations (
                    id TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
INSERT INTO schema_migrations VALUES('20260702_object_identity_v1','2024-05-06T07:08:00+00:00');
INSERT INTO schema_migrations VALUES('20260715_run_retention_v1','2024-05-06T07:08:00+00:00');
INSERT INTO schema_migrations VALUES('20260715_run_retention_delivery_v2','2024-05-06T07:08:00+00:00');
INSERT INTO schema_migrations VALUES('20260715_sync_event_telemetry_v1','2024-05-06T07:08:00+00:00');
CREATE TABLE envs (
                    name TEXT PRIMARY KEY,
                    python TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
INSERT INTO envs VALUES('fixture-env','/fixture/python','2024-05-06T07:08:09+00:00','2024-05-06T07:08:09+00:00');
CREATE TABLE objects (
                    id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    library TEXT NOT NULL DEFAULT 'default',
                    name TEXT NOT NULL,
                    kind TEXT,
                    origin TEXT NOT NULL DEFAULT 'local',
                    remote_owner_id TEXT,
                    remote_object_id TEXT,
                    source_object_name TEXT,
                    remote_name TEXT GENERATED ALWAYS AS (source_object_name) VIRTUAL,
                    description TEXT NOT NULL DEFAULT '',
                    current_version_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(owner_id, library, name)
                );
INSERT INTO objects VALUES('fixture-local-object','fixture-owner','default','fixture_function','function','local',NULL,NULL,NULL,'Credential-free 0.4.5 migration fixture','fixture-local-version-2','2024-05-06T07:08:09+00:00','2024-05-06T07:08:11+00:00');
INSERT INTO objects VALUES('fixture-pipeline-object','fixture-owner','default','fixture_pipeline','pipeline','local',NULL,NULL,NULL,'Pipeline migration fixture','fixture-pipeline-version','2024-05-06T07:08:16+00:00','2024-05-06T07:08:16+00:00');
CREATE TABLE object_versions (
                    id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    version_label TEXT,
                    description TEXT NOT NULL DEFAULT '',
                    entrypoint TEXT NOT NULL,
                    env TEXT NOT NULL,
                    env_python TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    yaml_text TEXT NOT NULL,
                    yaml_sha256 TEXT NOT NULL,
                    content_hash TEXT,
                    metadata_json TEXT NOT NULL,
                    inputs_json TEXT NOT NULL,
                    outputs_json TEXT NOT NULL,
                    pipeline_nodes_json TEXT NOT NULL,
                    distributions_json TEXT NOT NULL,
                    runtime_config_json TEXT NOT NULL DEFAULT '{"mode":"venv"}',
                    workdir TEXT,
                    remote_owner_id TEXT,
                    remote_object_id TEXT,
                    remote_version_id TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(object_id) REFERENCES objects(id),
                    FOREIGN KEY(env) REFERENCES envs(name),
                    UNIQUE(object_id, version),
                    UNIQUE(object_id, content_hash)
                );
INSERT INTO object_versions VALUES('fixture-local-version-1','fixture-local-object',1,NULL,'Credential-free 0.4.5 migration fixture','fixture_function','fixture-env','/fixture/python','function',unistr('- !DFunction\u000a  name: fixture_function\u000a  inputs: []\u000a  outputs:\u000a  - name: default\u000a    type: int\u000a  body: |-\u000a    return 1\u000a'),'8c2e95f6fb45dd6e4963a590f574656a3612b55f07b32f8c12efc2d5f63f4c23','1611bcdf5fbaca3a163e369a073b5134b140fd5346edfeb3dda33063c9a6c809','{"distributions": [], "entrypoint": "fixture_function", "imports": [], "inputs": [], "internal_objects": [{"inputs": [], "kind": "function", "name": "fixture_function", "outputs": [{"name": "default", "type": "int"}]}], "kind": "function", "outputs": [{"name": "default", "type": "int"}], "pipeline_nodes": []}','[]','[{"name": "default", "type": "int"}]','[]','[]','{"mode": "venv"}',NULL,NULL,NULL,NULL,'2024-05-06T07:08:10+00:00');
INSERT INTO object_versions VALUES('fixture-local-version-2','fixture-local-object',2,NULL,'Credential-free 0.4.5 migration fixture','fixture_function','fixture-env','/fixture/python','function',unistr('- !DFunction\u000a  name: fixture_function\u000a  inputs: []\u000a  outputs:\u000a  - name: default\u000a    type: int\u000a  body: |-\u000a    return 2\u000a'),'ec8736dee59a6aaa5725cc9a7e3ff3b2f1a9b80a612e5353b9b4d4a98966abe1','b18dff861a65823061c8e63592e9b686d5b39246712b19ead426a6dfc3f34ef8','{"distributions": [], "entrypoint": "fixture_function", "imports": [], "inputs": [], "internal_objects": [{"inputs": [], "kind": "function", "name": "fixture_function", "outputs": [{"name": "default", "type": "int"}]}], "kind": "function", "outputs": [{"name": "default", "type": "int"}], "pipeline_nodes": []}','[]','[{"name": "default", "type": "int"}]','[]','[]','{"mode": "venv"}',NULL,NULL,NULL,NULL,'2024-05-06T07:08:11+00:00');
INSERT INTO object_versions VALUES('fixture-pipeline-version','fixture-pipeline-object',1,NULL,'Pipeline migration fixture','fixture_pipeline','fixture-env','/fixture/python','pipeline',unistr('- !DPipeline\u000a  name: fixture_pipeline\u000a  nodes:\u000a  - !DNodeFunction\u000a    uuid: 00000000-0000-0000-0000-000000000001\u000a    func: fixture_step\u000a  links: []\u000a  aliases:\u000a  - [result, 00000000-0000-0000-0000-000000000001]\u000a- !DFunction\u000a  name: fixture_step\u000a  inputs: []\u000a  outputs:\u000a  - name: default\u000a    type: int\u000a  body: |-\u000a    return 7\u000a'),'02fd2479b4e4189d736bf5d08396b6da2c59fc00b7b5af5231bea81249bbbd3b','708fc8889d660ad579d8c46b164d79fc803b06a29680ad26c3ac7d18af4204e6','{"aliases": [{"name": "result", "node_id": "00000000-0000-0000-0000-000000000001"}], "distributions": [], "entrypoint": "fixture_pipeline", "imports": [], "inputs": [], "internal_objects": [{"inputs": [], "kind": "function", "name": "fixture_step", "outputs": [{"name": "default", "type": "int"}]}], "kind": "pipeline", "links": [], "outputs": [{"function": "fixture_step", "name": "result", "node_id": "00000000-0000-0000-0000-000000000001", "ports": [{"name": "default", "type": "int"}]}], "pipeline_nodes": [{"function": "fixture_step", "id": "00000000-0000-0000-0000-000000000001", "inputs": [], "kind": "function", "name": "fixture_step", "outputs": [{"name": "default", "type": "int"}]}]}','[]','[{"function": "fixture_step", "name": "result", "node_id": "00000000-0000-0000-0000-000000000001", "ports": [{"name": "default", "type": "int"}]}]','[{"function": "fixture_step", "id": "00000000-0000-0000-0000-000000000001", "inputs": [], "kind": "function", "name": "fixture_step", "outputs": [{"name": "default", "type": "int"}]}]','[]','{"mode": "venv"}',NULL,NULL,NULL,NULL,'2024-05-06T07:08:16+00:00');
CREATE TABLE object_functions (
                    id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    object_version_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    node_id TEXT,
                    name TEXT NOT NULL,
                    inputs_json TEXT NOT NULL,
                    outputs_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(object_id) REFERENCES objects(id),
                    FOREIGN KEY(object_version_id) REFERENCES object_versions(id)
                );
INSERT INTO object_functions VALUES('fixture-local-function-1','fixture-local-object','fixture-local-version-1','top_level',NULL,'fixture_function','[]','[{"name": "default", "type": "int"}]','{"distributions": [], "entrypoint": "fixture_function", "imports": [], "inputs": [], "internal_objects": [{"inputs": [], "kind": "function", "name": "fixture_function", "outputs": [{"name": "default", "type": "int"}]}], "kind": "function", "outputs": [{"name": "default", "type": "int"}], "pipeline_nodes": []}','2024-05-06T07:08:11+00:00');
INSERT INTO object_functions VALUES('fixture-local-function-2','fixture-local-object','fixture-local-version-2','top_level',NULL,'fixture_function','[]','[{"name": "default", "type": "int"}]','{"distributions": [], "entrypoint": "fixture_function", "imports": [], "inputs": [], "internal_objects": [{"inputs": [], "kind": "function", "name": "fixture_function", "outputs": [{"name": "default", "type": "int"}]}], "kind": "function", "outputs": [{"name": "default", "type": "int"}], "pipeline_nodes": []}','2024-05-06T07:08:12+00:00');
INSERT INTO object_functions VALUES('fixture-pipeline-function-1','fixture-pipeline-object','fixture-pipeline-version','pipeline_component','00000000-0000-0000-0000-000000000001','fixture_step','[]','[{"name": "default", "type": "int"}]','{"function": "fixture_step", "id": "00000000-0000-0000-0000-000000000001", "inputs": [], "kind": "function", "name": "fixture_step", "outputs": [{"name": "default", "type": "int"}]}','2024-05-06T07:08:16+00:00');
CREATE TABLE object_pipeline_nodes (
                    id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    object_version_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    node_kind TEXT NOT NULL,
                    name TEXT NOT NULL,
                    function_name TEXT,
                    remote_json TEXT NOT NULL,
                    inputs_json TEXT NOT NULL,
                    outputs_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(object_id) REFERENCES objects(id),
                    FOREIGN KEY(object_version_id) REFERENCES object_versions(id),
                    UNIQUE(object_version_id, node_id)
                );
INSERT INTO object_pipeline_nodes VALUES('fixture-pipeline-node-1','fixture-pipeline-object','fixture-pipeline-version','00000000-0000-0000-0000-000000000001','function','fixture_step','fixture_step','{}','[]','[{"name": "default", "type": "int"}]','{"function": "fixture_step", "id": "00000000-0000-0000-0000-000000000001", "inputs": [], "kind": "function", "name": "fixture_step", "outputs": [{"name": "default", "type": "int"}]}','2024-05-06T07:08:16+00:00');
CREATE TABLE object_pipeline_links (
                    id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    object_version_id TEXT NOT NULL,
                    target_node_id TEXT NOT NULL,
                    target_port TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_node_id TEXT,
                    source_port TEXT,
                    scalar_json TEXT,
                    link_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(object_id) REFERENCES objects(id),
                    FOREIGN KEY(object_version_id) REFERENCES object_versions(id)
                );
CREATE TABLE environment_builds (
                    spec_hash TEXT PRIMARY KEY,
                    base_python TEXT NOT NULL,
                    python_version TEXT NOT NULL,
                    distributions_json TEXT NOT NULL,
                    runtime_packages_json TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    venv_path TEXT NOT NULL,
                    python_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    error TEXT,
                    install_log_path TEXT,
                    builder TEXT,
                    runtime_type TEXT NOT NULL DEFAULT 'venv',
                    image_tag TEXT,
                    base_image TEXT
                );
INSERT INTO environment_builds VALUES('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','/fixture/python','Python 3.13.0','[{"package": "fixture-package", "version": "1.0"}]','[{"package": "pyyaml", "version": "6.0"}]','{"fixture": true, "runtime_type": "venv"}','/fixture/environment/venv','/fixture/environment/venv/bin/python','ready','2024-05-06T07:08:09+00:00','2024-05-06T07:08:09+00:00',NULL,NULL,NULL,'/fixture/environment/install.log','pip','venv',NULL,NULL);
CREATE TABLE runs (
                    id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    object_version_id TEXT NOT NULL,
                    object_name TEXT NOT NULL,
                    object_version INTEGER NOT NULL,
                    entrypoint TEXT NOT NULL,
                    env TEXT NOT NULL,
                    env_python TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    run_dir TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    result_path TEXT NOT NULL,
                    result_json TEXT,
                    artifacts_dir TEXT NOT NULL,
                    env_build_hash TEXT,
                    runtime_config_json TEXT NOT NULL DEFAULT '{"mode":"venv"}',
                    runtime_build_hash TEXT,
                    resolved_runtime TEXT,
                    runtime_backend TEXT,
                    image_tag TEXT,
                    container_id TEXT,
                    resolved_python TEXT,
                    interpreter_substitution_json TEXT,
                    error TEXT,
                    returncode INTEGER,
                    command_json TEXT,
                    stdout_path TEXT,
                    stderr_path TEXT,
                    stdout_text TEXT,
                    stderr_text TEXT,
                    keep TEXT NOT NULL DEFAULT 'on_failure',
                    manifest_json TEXT,
                    retention_enforced INTEGER NOT NULL DEFAULT 0,
                    retention_report_mode TEXT NOT NULL DEFAULT 'legacy',
                    retention_sync_required INTEGER NOT NULL DEFAULT 0,
                    retention_terminal_queued INTEGER NOT NULL DEFAULT 0,
                    retention_delivery_required INTEGER NOT NULL DEFAULT 0,
                    retention_delivery_acked INTEGER NOT NULL DEFAULT 0,
                    retention_delivery_expires_at TEXT,
                    retention_effective_status TEXT,
                    retention_outcome_reason TEXT,
                    FOREIGN KEY(object_id) REFERENCES objects(id),
                    FOREIGN KEY(object_version_id) REFERENCES object_versions(id)
                );
INSERT INTO runs VALUES('fixture-local-run','fixture-local-object','fixture-local-version-1','fixture_function',1,'fixture_function','fixture-env','/fixture/python','succeeded','2024-05-06T07:08:12+00:00',NULL,'2024-05-06T07:08:15+00:00','/fixture/runs/fixture-local-run','{"args": [[1, 2, 3]], "keep": true, "kwargs": {}, "output": null, "report_local_run": true, "runtime_config": {"mode": "venv"}, "timeout_seconds": null}','/fixture/runs/fixture-local-run/result.json','{"default": 1}','/fixture/runs/fixture-local-run/artifacts','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','{"mode": "venv"}',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,'/fixture/runs/fixture-local-run/stdout.log','/fixture/runs/fixture-local-run/stderr.log',NULL,NULL,'true','{"created_at": "2024-05-06T07:08:12+00:00", "edges": [], "finished_at": "2024-05-06T07:08:15+00:00", "inputs": {}, "keep": true, "nodes": {}, "parent_run_id": null, "pipeline": {"content_hash": "1611bcdf5fbaca3a163e369a073b5134b140fd5346edfeb3dda33063c9a6c809", "entrypoint": "fixture_function", "name": "fixture_function", "object_version_id": "fixture-local-version-1"}, "retention": {"class": "keep", "expires_at": null}, "run_id": "fixture-local-run", "schema_version": 1, "started_at": "2024-05-06T07:08:12+00:00", "status": "succeeded"}',1,'local',0,0,1,0,NULL,NULL,NULL);
INSERT INTO runs VALUES('fixture-failed-run','fixture-pipeline-object','fixture-pipeline-version','fixture_pipeline',1,'fixture_pipeline','fixture-env','/fixture/python','failed','2024-05-06T07:08:17+00:00',NULL,'2024-05-06T07:08:18+00:00','/fixture/runs/fixture-failed-run','{"args": [], "keep": true, "kwargs": {}, "output": null, "report_local_run": true, "runtime_config": {"mode": "venv"}, "timeout_seconds": null}','/fixture/runs/fixture-failed-run/result.json',NULL,'/fixture/runs/fixture-failed-run/artifacts','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','{"mode": "venv"}',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'synthetic retained failure',1,NULL,'/fixture/runs/fixture-failed-run/stdout.log','/fixture/runs/fixture-failed-run/stderr.log',NULL,NULL,'true','{"created_at": "2024-05-06T07:08:17+00:00", "edges": [], "finished_at": "2024-05-06T07:08:18+00:00", "inputs": {}, "keep": true, "nodes": {}, "parent_run_id": null, "pipeline": {"content_hash": "708fc8889d660ad579d8c46b164d79fc803b06a29680ad26c3ac7d18af4204e6", "entrypoint": "fixture_pipeline", "name": "fixture_pipeline", "object_version_id": "fixture-pipeline-version"}, "retention": {"class": "keep", "expires_at": null}, "run_id": "fixture-failed-run", "schema_version": 1, "started_at": "2024-05-06T07:08:17+00:00", "status": "failed"}',1,'local',0,0,1,0,NULL,NULL,NULL);
CREATE TABLE server_connections (
                    id TEXT PRIMARY KEY,
                    server_url TEXT NOT NULL,
                    token_hint TEXT NOT NULL,
                    user_token_hint TEXT,
                    token_secret_ref TEXT,
                    user_token_secret_ref TEXT,
                    token_redacted TEXT NOT NULL,
                    user_token_redacted TEXT,
                    remote_connection_id TEXT,
                    owner_id TEXT,
                    subject_type TEXT,
                    subject_id TEXT,
                    machine_id TEXT NOT NULL,
                    display_name TEXT,
                    capabilities_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    heartbeat_interval_seconds REAL NOT NULL DEFAULT 60,
                    last_heartbeat_at TEXT,
                    next_heartbeat_at TEXT,
                    lease_expires_at TEXT,
                    last_library_snapshot_hash TEXT,
                    last_library_snapshot_at TEXT,
                    created_at TEXT NOT NULL,
                    connected_at TEXT,
                    disconnected_at TEXT,
                    updated_at TEXT NOT NULL,
                    error TEXT
                );
CREATE TABLE sync_events (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    retryable INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    sent_at TEXT,
                    error TEXT,
                    local_run_id TEXT,
                    payload_expires_at TEXT,
                    FOREIGN KEY(local_run_id) REFERENCES runs(id) ON DELETE CASCADE
                );
INSERT INTO sync_events VALUES('fixture-sync-pending','object_version','{"content_hash": "1611bcdf5fbaca3a163e369a073b5134b140fd5346edfeb3dda33063c9a6c809", "owner_id": "fixture-owner", "source_object_id": "fixture-local-object", "source_version_id": "fixture-local-version-1"}','pending',0,1,'2024-05-06T07:08:13+00:00','2024-05-06T07:08:13+00:00',NULL,NULL,NULL,NULL);
INSERT INTO sync_events VALUES('fixture-sync-sent','local_run_update','{"run": {"id": "fixture-local-run", "status": "queued"}}','sent',0,1,'2024-05-06T07:08:14+00:00','2024-05-06T07:08:15+00:00','2024-05-06T07:08:15+00:00',NULL,'fixture-local-run','2024-05-13T07:08:15+00:00');
CREATE TABLE remote_signatures (
                    id TEXT PRIMARY KEY,
                    server_url TEXT NOT NULL,
                    owner_id TEXT,
                    library TEXT,
                    object_name TEXT NOT NULL,
                    version TEXT,
                    version_id TEXT,
                    signature_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    fetched_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
CREATE INDEX idx_objects_name
                    ON objects(name);
CREATE INDEX idx_object_versions_object
                    ON object_versions(object_id, version);
CREATE INDEX idx_object_functions_version
                    ON object_functions(object_version_id, role, node_id);
CREATE INDEX idx_object_pipeline_nodes_version
                    ON object_pipeline_nodes(object_version_id, node_id);
CREATE INDEX idx_object_pipeline_links_version
                    ON object_pipeline_links(object_version_id, target_node_id, target_port);
CREATE INDEX idx_runs_created
                    ON runs(created_at);
CREATE INDEX idx_environment_builds_status
                    ON environment_builds(status);
CREATE INDEX idx_server_connections_status
                    ON server_connections(status, updated_at);
CREATE INDEX idx_remote_signatures_ref
                    ON remote_signatures(server_url, owner_id, library, object_name, version, version_id);
CREATE UNIQUE INDEX idx_objects_identity
                ON objects(owner_id, library, name)
                ;
CREATE UNIQUE INDEX idx_object_versions_content_hash
                ON object_versions(object_id, content_hash)
                WHERE content_hash IS NOT NULL
                ;
CREATE UNIQUE INDEX idx_objects_remote_object
                ON objects(remote_object_id)
                WHERE remote_object_id IS NOT NULL
                ;
CREATE UNIQUE INDEX idx_object_versions_remote_version
                ON object_versions(remote_version_id)
                WHERE remote_version_id IS NOT NULL
                ;
CREATE INDEX idx_sync_events_local_run
                ON sync_events(local_run_id, status)
                ;
CREATE INDEX idx_sync_events_payload_expiry
                ON sync_events(payload_expires_at)
                ;
PRAGMA user_version=4;
COMMIT;

