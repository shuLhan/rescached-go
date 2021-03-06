// Copyright 2018, Shulhan <ms@kilabit.info>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/shuLhan/share/lib/dns"
)

// List of error messages.
var (
	errQueryName  = errors.New("invalid or empty query name")
	errQueryType  = errors.New("unknown query type")
	errQueryClass = errors.New("unknown query class")
)

// List of command line usages.
const (
	usageInsecure   = `Skip verifying server certificate`
	usageNameServer = "Parent name server address using scheme based.\n" +
		"\tFor example,\n" +
		"\tudp://35.240.172.103:53 for querying with UDP,\n" +
		"\ttcp://35.240.172.103:53 for querying with TCP,\n" +
		"\thttps://35.240.172:103:853 for querying with DNS over TLS, and\n" +
		"\thttps://kilabit.info/dns-query for querying with DNS over HTTPS."

	usageType = "Query type.  Valid values are either A, NS, CNAME, SOA,\n" +
		"\tMB, MG, MR, NULL, WKS, PTR, HINFO, MINFO, MX, TXT, AAAA, or SRV.\n" +
		"\tDefault value is A."

	usageClass = "Query class.  Valid values are either IN, CS, HS.\n" +
		"\tDefault value is IN."
)

type options struct {
	sqtype  string
	sqclass string

	nameserver string
	insecure   bool
	qname      string
	qtype      uint16
	qclass     uint16
}

func help() {
	fmt.Println(`
= resolver: command line interface for DNS query

==  Usage

	resolver [-ns nameserver] [-insecure] [-t string] [-c string] [domain|address]

==  Options

-ns nameserver

	` + usageNameServer + `

-insecure

	` + usageInsecure + `

-t string

	` + usageType + `

-c string

	` + usageClass + `

==  Examples

Query the MX records using UDP on name server 35.240.172.103,

	$ resolver -ns udp://35.240.172.103 -t MX kilabit.info

Query the IPv4 records of domain name "kilabit.info" using DNS over TLS on
name server 35.240.172.103,

	$ resolver -ns https://35.240.172.103 -insecure kilabit.info

Query the IPv4 records of domain name "kilabit.info" using DNS over HTTPS on
name server kilabit.info,

	$ resolver -ns https://kilabit.info/dns-query kilabit.info
`)
}

func newOptions() (*options, error) {
	var optHelp bool

	opts := new(options)

	flag.StringVar(&opts.nameserver, "ns", "", usageNameServer)
	flag.BoolVar(&opts.insecure, "insecure", false, usageInsecure)
	flag.BoolVar(&optHelp, "h", false, "")
	flag.StringVar(&opts.sqtype, "t", "A", usageType)
	flag.StringVar(&opts.sqclass, "c", "IN", usageClass)

	flag.Parse()

	args := flag.Args()

	if optHelp {
		help()
		os.Exit(1)
	}

	if len(args) == 0 {
		help()
		os.Exit(1)
	}

	opts.qname = args[0]

	err := opts.parseQType()
	if err != nil {
		help()
		os.Exit(1)
	}

	err = opts.parseQClass()
	if err != nil {
		help()
		os.Exit(1)
	}

	return opts, nil
}

func (opts *options) parseQType() error {
	var ok bool

	opts.sqtype = strings.ToUpper(opts.sqtype)

	opts.qtype, ok = dns.QueryTypes[opts.sqtype]
	if !ok {
		return errQueryType
	}

	return nil
}

func (opts *options) parseQClass() error {
	var ok bool

	opts.sqclass = strings.ToUpper(opts.sqclass)

	opts.qclass, ok = dns.QueryClasses[opts.sqclass]
	if !ok {
		return errQueryClass
	}

	return nil
}
