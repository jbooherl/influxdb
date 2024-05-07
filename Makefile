export PKG_CONFIG:=$(shell pwd)/pkg-config.sh

build:
	go build ./...
