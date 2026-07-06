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

// Interfaces Log and DB are defined in internal/gp/lister and re-exported
// here as type aliases so that hand-written mocks in this package remain
// compatible and the go:generate directive below stays meaningful.
//
//go:generate mockgen -source=../lister/deps.go -package=stat_activity_test -mock_names DB=MockDB,Log=MockLog -destination mocks_test.go

package stat_activity
