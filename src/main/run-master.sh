# !/bin/bash

# build the word count plugin
go build -buildmode=plugin ../mrapps/wc.go
#go build -race -buildmode=plugin ../mrapps/wc.go

# clean up old output
rm mr-*

# run master at the main folder
go run mrmaster.go pg-*.txt
#go run -race mrmaster.go pg-*.txt

# after all workers and master have finished,  view the output:
# cat mr-out-* | sort | more
