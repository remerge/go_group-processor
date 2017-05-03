PROJECT := go-group_processor
PACKAGE := github.com/remerge/$(PROJECT)

GOMETALINTER_OPTS = --enable-all --tests --fast -D golint

include Makefile.common
