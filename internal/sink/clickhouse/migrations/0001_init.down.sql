{{if .Replicated}}
DROP TABLE IF EXISTS yagpcc.sessions ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS yagpcc.statements ON CLUSTER '{cluster}';
DROP TABLE IF EXISTS yagpcc.segments ON CLUSTER '{cluster}';
{{end}}
DROP TABLE IF EXISTS yagpcc.segments_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}};
DROP TABLE IF EXISTS yagpcc.statements_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}};
DROP TABLE IF EXISTS yagpcc.sessions_part{{if .Replicated}} ON CLUSTER '{cluster}'{{end}};
DROP TABLE IF EXISTS yagpcc._yagpcc_meta;
