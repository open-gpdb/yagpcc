// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the
// License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestArchiveJSONLimit(t *testing.T) {
	for _, text := range []string{
		strings.Repeat("x", 2048),
		strings.Repeat("\\\"\n\t<>&", 400),
		strings.Repeat("Запрос🙂", 400),
	} {
		in, err := json.Marshal(map[string]interface{}{
			"GpStatInfo":       map[string]interface{}{"Query": text},
			"RunningQueryInfo": map[string]interface{}{"QueryText": text, "PlanText": "short plan", "QueryID": uint64(18446744073709551615)},
			"metric":           uint64(9007199254740993),
		})
		require.NoError(t, err)
		original := bytes.Clone(in)
		out, changed, err := limitArchiveJSON(in, 1024)
		require.NoError(t, err)
		require.True(t, changed)
		require.LessOrEqual(t, len(out)+1, 1024)
		require.True(t, json.Valid(out))
		require.True(t, utf8.Valid(out))
		require.Equal(t, original, in)
		var record map[string]interface{}
		dec := json.NewDecoder(bytes.NewReader(out))
		dec.UseNumber()
		require.NoError(t, dec.Decode(&record))
		require.Equal(t, json.Number("9007199254740993"), record["metric"])
		query := record["RunningQueryInfo"].(map[string]interface{})
		require.Equal(t, json.Number("18446744073709551615"), query["QueryID"])
		require.Equal(t, "short plan", query["PlanText"])
		clipped := query["QueryText"].(string)
		require.True(t, strings.HasSuffix(clipped, truncationMarker))
		require.True(t, utf8.ValidString(clipped))
		require.True(t, strings.HasPrefix(text, strings.TrimSuffix(clipped, truncationMarker)))
	}
}

func TestArchiveJSONBoundary(t *testing.T) {
	in := []byte(`{"GpStatInfo":{"Query":"` + strings.Repeat("x", 200) + `"}}`)
	for _, limit := range []int{len(in) + 2, len(in) + 1, len(in)} {
		out, changed, err := limitArchiveJSON(in, limit)
		require.NoError(t, err)
		require.LessOrEqual(t, len(out)+1, limit)
		require.Equal(t, limit < len(in)+1, changed)
		if !changed {
			require.Equal(t, in, out)
		}
	}
}

func TestArchiveJSONDoesNotTruncateOtherFields(t *testing.T) {
	in := []byte(`{"message":"` + strings.Repeat("x", 2000) + `","queryInfo":{"queryText":"select 1"}}`)
	out, _, err := limitArchiveJSON(in, 512)
	require.Error(t, err)
	require.Nil(t, out)
}

func TestFileWriterLimitsAllStreamsAndPreservesSource(t *testing.T) {
	ctx := context.Background()
	text := strings.Repeat("🙂\"\\", FileRecordLimit/4)
	session := &gp.SessionDataWrite{
		GpStatInfo:       &gp.GpStatActivity{Query: &text},
		RunningQueryInfo: &gp.QueryInfoShort{QueryText: text, PlanText: text, QueryID: 18446744073709551615},
	}
	query := &pbm.QueryStatWrite{QueryInfo: &pbc.QueryInfo{QueryText: text, PlanText: text, TemplateQueryText: text, TemplatePlanText: text}}
	segment := &pbm.SegmentMetricsWrite{QueryInfo: query.QueryInfo}
	var sessions, queries, segments bytes.Buffer
	fw := &FileWriters{logger: zap.NewNop().Sugar(), sessionWriter: &sessions, queryWriter: &queries, segmentWriter: &segments}
	beforeSession, err := session.ToJSON()
	require.NoError(t, err)
	beforeQuery, err := (&QueryStatWriteSerializable{v: query}).ToJSON()
	require.NoError(t, err)
	require.NoError(t, fw.StoreSessions(ctx, []*gp.SessionDataWrite{session, session}))
	require.NoError(t, fw.StoreQuery(ctx, []*pbm.QueryStatWrite{query}))
	require.NoError(t, fw.StoreSegmensMetrics(ctx, []*pbm.SegmentMetricsWrite{segment}))
	for _, buf := range []*bytes.Buffer{&sessions, &queries, &segments} {
		require.True(t, bytes.HasSuffix(buf.Bytes(), []byte{'\n'}))
		for _, line := range bytes.Split(bytes.TrimSuffix(buf.Bytes(), []byte{'\n'}), []byte{'\n'}) {
			require.True(t, json.Valid(line))
			require.LessOrEqual(t, len(line)+1, FileRecordLimit)
			require.Contains(t, string(line), truncationMarker)
		}
	}
	require.Equal(t, 2, bytes.Count(sessions.Bytes(), []byte{'\n'}))
	afterSession, err := session.ToJSON()
	require.NoError(t, err)
	afterQuery, err := (&QueryStatWriteSerializable{v: query}).ToJSON()
	require.NoError(t, err)
	require.JSONEq(t, string(beforeSession), string(afterSession))
	require.JSONEq(t, string(beforeQuery), string(afterQuery))
	require.Equal(t, text, segment.QueryInfo.QueryText)
}

func TestFileWriterContinuesAfterUntrimmableRecord(t *testing.T) {
	var out bytes.Buffer
	fw := &FileWriters{logger: zap.NewNop().Sugar(), queryWriter: &out}
	require.NoError(t, fw.StoreQuery(context.Background(), []*pbm.QueryStatWrite{
		{Message: strings.Repeat("x", FileRecordLimit)},
		{QueryInfo: &pbc.QueryInfo{QueryText: "select 1"}},
	}))
	require.Equal(t, 1, bytes.Count(out.Bytes(), []byte{'\n'}))
	require.Contains(t, out.String(), "select 1")
}
