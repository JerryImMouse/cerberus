CREATE TABLE incidents (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_key  TEXT    NOT NULL,
    ts            INTEGER NOT NULL,
    kind          TEXT    NOT NULL,   -- start | exit | heartbeat_lost | admin | spawn_failed
    detail        TEXT                -- free-form: exit code, reason, admin action, etc.
);
CREATE INDEX idx_incidents_key_ts ON incidents (instance_key, ts DESC);
