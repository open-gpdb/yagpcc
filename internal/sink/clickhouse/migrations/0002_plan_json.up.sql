ALTER TABLE yagpcc.statements_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}}
    ADD COLUMN IF NOT EXISTS `plan_json` Nullable(String) CODEC(ZSTD(3)) AFTER `plan_text`;

ALTER TABLE yagpcc.statements_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}}
    ADD COLUMN IF NOT EXISTS `analyze_json` Nullable(String) CODEC(ZSTD(3)) AFTER `plan_json`;

ALTER TABLE yagpcc.segments_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}}
    ADD COLUMN IF NOT EXISTS `plan_json` Nullable(String) CODEC(ZSTD(3)) AFTER `plan_text`;

ALTER TABLE yagpcc.segments_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}}
    ADD COLUMN IF NOT EXISTS `analyze_json` Nullable(String) CODEC(ZSTD(3)) AFTER `plan_json`;
{{if .Replicated}}
ALTER TABLE yagpcc.statements ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `plan_json` Nullable(String) CODEC(ZSTD(3)) AFTER `plan_text`;

ALTER TABLE yagpcc.statements ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `analyze_json` Nullable(String) CODEC(ZSTD(3)) AFTER `plan_json`;

ALTER TABLE yagpcc.segments ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `plan_json` Nullable(String) CODEC(ZSTD(3)) AFTER `plan_text`;

ALTER TABLE yagpcc.segments ON CLUSTER '{cluster}'
    ADD COLUMN IF NOT EXISTS `analyze_json` Nullable(String) CODEC(ZSTD(3)) AFTER `plan_json`;
{{end}}
