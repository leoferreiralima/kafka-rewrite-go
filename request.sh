#!/bin/sh

cat $1 | xxd -r -p | nc localhost 9092 | hexdump -C | awk '{ print $1 }'