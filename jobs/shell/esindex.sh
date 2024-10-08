#!/bin/bash


index=2024-09-12
# wildcard 사용 금지 해제
curl -XPUT \
      "http://localhost:9200/_cluster/settings?pretty" \
      -H 'Content-Type: application/json' \
      -d '{ "persistent": { "action.destructive_requires_name": false } }'

# index 패턴 삽입
curl -XDELETE "http://localhost:9200/*" 

# wildcard 사용 금지
curl -XPUT \
      "http://localhost:9200/_cluster/settings?pretty" \
      -H 'Content-Type: application/json' \
      -d '{ "persistent": { "action.destructive_requires_name": true } }'
