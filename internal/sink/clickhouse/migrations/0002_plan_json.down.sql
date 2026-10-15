{{if .Replicated}}
ALTER TABLE yagpcc.segments ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `analyze_json`;

ALTER TABLE yagpcc.segments ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `plan_json`;

ALTER TABLE yagpcc.statements ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `analyze_json`;

ALTER TABLE yagpcc.statements ON CLUSTER '{cluster}'
    DROP COLUMN IF EXISTS `plan_json`;
{{end}}
ALTER TABLE yagpcc.segments_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}}
    DROP COLUMN IF EXISTS `analyze_json`;

ALTER TABLE yagpcc.segments_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}}
    DROP COLUMN IF EXISTS `plan_json`;

ALTER TABLE yagpcc.statements_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}}
    DROP COLUMN IF EXISTS `analyze_json`;

ALTER TABLE yagpcc.statements_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}}
    DROP COLUMN IF EXISTS `plan_json`;
