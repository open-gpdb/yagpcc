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

package master_sentinel

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (s *Sentinel) RunUntilIsMaster(ctx context.Context) error {
	t := time.NewTicker(s.checkInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			isInRecovery, err := s.isInRecovery(ctx)
			if err != nil {
				s.subsequentCheckErrorsN++
				if s.subsequentCheckErrorsN >= s.maxSubsequentCheckErrorsN {
					return fmt.Errorf(
						"exceeded number of max subsequent check errors (maxSubsequentCheckErrorsN=%d), "+
							"last error was: %w",
						s.maxSubsequentCheckErrorsN,
						err,
					)
				}

				s.log.Warnf(
					"check is in recovery error suppressed (subsequentCheckErrorsN=%d): %s",
					s.subsequentCheckErrorsN,
					err.Error(),
				)

				continue
			}

			s.subsequentCheckErrorsN = 0
			if isInRecovery {
				return fmt.Errorf("current instance is in recovery mode")
			}
		}
	}
}

func NewSentinel(log log, db db, opts ...Option) *Sentinel {
	const (
		defaultCheckInterval             = 5 * time.Second
		defaultCheckTimeout              = 3 * time.Second
		defaultMaxSubsequentCheckErrorsN = 6
	)

	s := &Sentinel{
		log:                       log,
		db:                        db,
		checkInterval:             defaultCheckInterval,
		checkTimeout:              defaultCheckTimeout,
		maxSubsequentCheckErrorsN: defaultMaxSubsequentCheckErrorsN,
	}

	for _, o := range opts {
		o(s)
	}

	return s
}

type Sentinel struct {
	log                       log
	db                        db
	checkInterval             time.Duration
	checkTimeout              time.Duration
	maxSubsequentCheckErrorsN int
	subsequentCheckErrorsN    int
}

type Option func(*Sentinel)

func WithCheckInterval(interval time.Duration) Option {
	return func(s *Sentinel) {
		s.checkInterval = interval
	}
}

func WithMaxSubsequentCheckErrorsN(n int) Option {
	return func(s *Sentinel) {
		s.maxSubsequentCheckErrorsN = n
	}
}

func (s *Sentinel) isInRecovery(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.checkTimeout)
	defer cancel()

	const query = `select pg_is_in_recovery();`

	var isInRecoveryQueryRows []bool
	if err := s.db.ExecQueryNoRetry(ctx, query, &isInRecoveryQueryRows); err != nil {
		if s.hostIsInRecoveryError(err) {
			return true, nil
		}

		return false, err
	}

	if len(isInRecoveryQueryRows) != 1 {
		return false, fmt.Errorf(
			"unexpected number of rows from database: got %d, but expected %d",
			len(isInRecoveryQueryRows),
			1,
		)
	}

	return isInRecoveryQueryRows[0], nil
}

func (s *Sentinel) hostIsInRecoveryError(err error) bool {
	const errorSubstring = "FATAL: the database system is in recovery mode (SQLSTATE 57M02)"
	return strings.Contains(err.Error(), errorSubstring)
}
