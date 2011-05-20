include $(GOROOT)/src/Make.inc

TARG=yadserver
GOFILES=\
	yadserver.go\
	update.go\
	serve.go\

include $(GOROOT)/src/Make.pkg
