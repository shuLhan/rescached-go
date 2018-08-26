## Copyright 2018, Shulhan <ms@kilabit.info>. All rights reserved.
## Use of this source code is governed by a BSD-style
## license that can be found in the LICENSE file.

.PHONY: install build test doc test.prof coverbrowse lint clean distclean

SRC:=$(shell go list -f '{{$$d:=.Dir}} {{ range .GoFiles }}{{$$d}}/{{.}} {{end}}' ./...)
SRC_TEST:=$(shell go list -f '{{$$d:=.Dir}} {{ range .TestGoFiles }}{{$$d}}/{{.}} {{end}}' ./...)

COVER_OUT:=cover.out
COVER_HTML:=cover.html
CPU_PROF:=cpu.prof
MEM_PROF:=mem.prof

RESCACHED_CFG:=./cmd/rescached/rescached.cfg
RESCACHED_CFG_MAN:=./rescached.cfg.5.gz

RESCACHED_BIN:=./rescached
RESCACHED_MAN:=./rescached.1.gz


build: test $(RESCACHED_BIN) doc

test: $(COVER_HTML)

test.prof:
	export CGO_ENABLED=1 && \
	go test -race -count=1 -cpuprofile $(CPU_PROF) -memprofile $(MEM_PROF) ./...

$(COVER_HTML): $(COVER_OUT)
	go tool cover -html=$< -o $@

$(COVER_OUT): $(SRC) $(SRC_TEST)
	export CGO_ENABLED=1 && \
	go test -race -count=1 -coverprofile=$@ ./...

coverbrowse: $(COVER_HTML)
	xdg-open $<

lint:
	golangci-lint run ./...

$(RESCACHED_BIN): $(SRC)
	export CGO_ENABLED=1 && \
	go build -race -v ./cmd/rescached

doc: $(RESCACHED_MAN) $(RESCACHED_CFG_MAN)

$(RESCACHED_MAN): README.adoc
	@a2x -v --doctype manpage --format manpage $< 2>/dev/null
	@gzip -f rescached.1

$(RESCACHED_CFG_MAN): rescached.cfg.adoc
	@a2x -v --doctype manpage --format manpage $< 2>/dev/null
	@gzip -f rescached.cfg.5

distclean: clean
	go clean -i ./...

clean:
	rm -f $(COVER_OUT) $(COVER_HTML)
	rm $(RESCACHED_BIN)

install: build
	sudo mkdir -p /etc/rescached
	sudo mkdir -p /etc/rescached/hosts.d
	sudo cp $(RESCACHED_CFG)    /etc/rescached/
	sudp cp scripts/hosts.block /etc/rescached/hosts.d/

	sudo mkdir -p /usr/bin
	sudo cp -f $(RESCACHED_BIN)                     /usr/bin/
	sudo cp scripts/rescached-update-hosts-block.sh /usr/bin/

	sudo mkdir -p /usr/share/man/man{1,5}
	sudo cp $(RESCACHED_MAN)     /usr/share/man/man1/
	sudo cp $(RESCACHED_CFG_MAN) /usr/share/man/man5/

	sudo mkdir -p /usr/share/rescached
	sudo cp LICENSE /usr/share/rescached/

uninstall:
	sudo rm /usr/bin/$(RESCACHED_BIN)
	sudo rm /usr/share/man/man1/$(RESCACHED_MAN)
	sudo rm /usr/share/man/man5/$(RESCACHED_CFG_MAN)
	sudo rm /usr/share/rescached/LICENSE
