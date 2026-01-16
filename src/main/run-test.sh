# !/bin/bash

rm ../mrapps/*.so
rm *.so
rm mrmaster
rm mrworker
rm mrsequential

# build the word count plugin
go build -buildmode=plugin ../mrapps/wc.go

# run the test script
sh ./test-mr.sh
