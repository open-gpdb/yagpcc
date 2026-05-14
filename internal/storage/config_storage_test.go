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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigStorage(t *testing.T) {
	SetHostnameForSegindex(1, "hostname1")
	fqdn1 := GetHostnameForSegindex(1)
	assert.Equal(t, fqdn1, "hostname1")
	fqdn2 := GetHostnameForSegindex(2)
	assert.Equal(t, fqdn2, "2")
}
