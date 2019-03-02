// Copyright 2018, Shulhan <ms@kilabit.info>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rescached

import (
	"container/list"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/shuLhan/share/lib/debug"
	"github.com/shuLhan/share/lib/dns"
)

//
// response represent internal DNS response for caching.
//
type response struct {
	// Time when message is received.
	receivedAt int64
	// Time when message last accessed in cache.
	accessedAt int64
	message    *dns.Message
	// Pointer to response element in list.
	el *list.Element
}

func newResponse(msg *dns.Message) *response {
	curtime := time.Now().Unix()
	return &response{
		receivedAt: curtime,
		accessedAt: curtime,
		message:    msg,
	}
}

//
// AccessedAt return the timestamp when response last accessed in cache.
//
func (res *response) AccessedAt() int64 {
	return atomic.LoadInt64(&res.accessedAt)
}

//
// String return the interpretation of response as text.
// The message field only representated by the Question section.
//
func (res *response) String() string {
	return fmt.Sprintf("{%d %d %s}", res.receivedAt, res.accessedAt,
		res.message.Question.String())
}

//
// isExpired will return true if response message is expired, otherwise
// it will return false.
// If response is not expired, all TTL in RR will be decreased to current time
// minus time they were received.
//
func (res *response) isExpired() bool {
	// Local responses from hosts file will never be expired.
	if res.receivedAt == 0 {
		return false
	}

	timeNow := time.Now().Unix()
	elapSeconds := uint32(timeNow - res.receivedAt)
	res.receivedAt = timeNow

	if res.message.IsExpired(elapSeconds) {
		if debug.Value >= 1 {
			fmt.Printf("- expired:  Elaps:%-4d ID:%-5d %s\n",
				elapSeconds,
				res.message.Header.ID, res.message.Question)
		}

		return true
	}

	res.message.SubTTL(elapSeconds)

	return false
}

func (res *response) update(newMsg *dns.Message) *dns.Message {
	oldMsg := res.message
	atomic.StoreInt64(&res.accessedAt, time.Now().Unix())
	res.message = newMsg
	return oldMsg
}
