#!/bin/sh

# dmclock
git subtree push \
    --prefix src/dmclock \
    git@github.com:ceph/dmclock.git K_way_heap

# add other subtree pull commands here...
