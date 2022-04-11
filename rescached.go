// SPDX-FileCopyrightText: 2018 M. Shulhan <ms@kilabit.info>
// SPDX-License-Identifier: GPL-3.0-or-later

// Package rescached implement DNS forwarder with cache.
package rescached

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/shuLhan/share/lib/debug"
	"github.com/shuLhan/share/lib/dns"
	"github.com/shuLhan/share/lib/http"
	"github.com/shuLhan/share/lib/memfs"
)

const (
	cachesDir  = "/var/cache/rescached/"
	cachesFile = "rescached.gob"
)

// Server implement caching DNS server.
type Server struct {
	dns       *dns.Server
	env       *Environment
	rcWatcher *memfs.Watcher

	httpd       *http.Server
	httpdRunner sync.Once
}

//
// New create and initialize new rescached server.
//
func New(env *Environment) (srv *Server, err error) {
	if debug.Value >= 1 {
		fmt.Printf("--- rescached: config: %+v\n", env)
	}

	err = env.init()
	if err != nil {
		return nil, fmt.Errorf("rescached: New: %w", err)
	}

	env.initHostsBlock()

	srv = &Server{
		env: env,
	}

	err = srv.httpdInit()
	if err != nil {
		return nil, err
	}

	return srv, nil
}

//
// Start the server, waiting for DNS query from clients, read it and response
// it.
//
func (srv *Server) Start() (err error) {
	logp := "Start"

	srv.dns, err = dns.NewServer(&srv.env.ServerOptions)
	if err != nil {
		return err
	}

	cachesPath := filepath.Join(cachesDir, cachesFile)

	fcaches, err := os.Open(cachesPath)
	if err == nil {
		// Load stored caches from file.
		answers, err := srv.dns.CachesLoad(fcaches)
		if err != nil {
			log.Printf("%s: %s", logp, err)
		} else {
			fmt.Printf("%s: %d caches loaded from %s\n", logp, len(answers), cachesPath)
		}

		err = fcaches.Close()
		if err != nil {
			log.Printf("%s: %s", logp, err)
		}
	}

	systemHostsFile, err := dns.ParseHostsFile(dns.GetSystemHosts())
	if err != nil {
		return err
	}
	err = srv.dns.PopulateCachesByRR(systemHostsFile.Records,
		systemHostsFile.Path)
	if err != nil {
		return err
	}

	srv.env.HostsFiles, err = dns.LoadHostsDir(dirHosts)
	if err != nil {
		return err
	}

	for _, hf := range srv.env.HostsFiles {
		err = srv.dns.PopulateCachesByRR(hf.Records, hf.Path)
		if err != nil {
			return err
		}
	}

	srv.env.Zones, err = dns.LoadZoneDir(dirZone)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		err = os.MkdirAll(dirZone, 0700)
		if err != nil {
			return err
		}
		err = nil
	}
	for _, zoneFile := range srv.env.Zones {
		srv.dns.PopulateCaches(zoneFile.Messages(), zoneFile.Path)
	}

	if len(srv.env.FileResolvConf) > 0 {
		go srv.watchResolvConf()
	}

	go func() {
		srv.httpdRunner.Do(srv.httpdRun)
	}()

	go srv.run()

	return nil
}

func (srv *Server) run() {
	defer func() {
		err := recover()
		if err != nil {
			log.Println("panic: ", err)
		}
	}()

	err := srv.dns.ListenAndServe()
	if err != nil {
		log.Println(err)
	}
}

//
// Stop the server.
//
func (srv *Server) Stop() {
	logp := "Stop"

	if srv.rcWatcher != nil {
		srv.rcWatcher.Stop()
	}
	srv.dns.Stop()

	cachesPath := filepath.Join(cachesDir, cachesFile)

	// Stores caches to file for next start.
	err := os.MkdirAll(cachesDir, 0700)
	if err != nil {
		log.Printf("%s: %s", logp, err)
		return
	}
	fcaches, err := os.Create(cachesPath)
	if err != nil {
		log.Printf("%s: %s", logp, err)
		return
	}
	n, err := srv.dns.CachesSave(fcaches)
	if err != nil {
		log.Printf("%s: %s", logp, err)
		// fall-through for Close.
	}
	err = fcaches.Close()
	if err != nil {
		log.Printf("%s: %s", logp, err)
	}
	fmt.Printf("%s: %d caches stored to %s\n", logp, n, cachesPath)
}

// watchResolvConf watch an update to file resolv.conf.
func (srv *Server) watchResolvConf() {
	var (
		logp = "watchResolvConf"

		ns  memfs.NodeState
		err error
	)

	srv.rcWatcher, err = memfs.NewWatcher(srv.env.FileResolvConf, 0)
	if err != nil {
		log.Fatalf("%s: %s", logp, err)
	}

	for ns = range srv.rcWatcher.C {
		switch ns.State {
		case memfs.FileStateDeleted:
			log.Printf("= %s: file %q deleted\n", logp, srv.env.FileResolvConf)
			return
		default:
			ok, err := srv.env.loadResolvConf()
			if err != nil {
				log.Printf("%s: %s", logp, err)
				break
			}
			if !ok {
				break
			}

			srv.dns.RestartForwarders(srv.env.NameServers)
		}
	}
}
