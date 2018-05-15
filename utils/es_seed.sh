#!/bin/sh
curl -XDELETE http://localhost:9200/honestbee -H "content-type: application/json" -d @index.json
curl -XPUT http://localhost:9200/honestbee -H "content-type: application/json" -d @index.json
