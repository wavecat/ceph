#!/bin/sh

# dmclock
git subtree pull \
    --prefix src/dmclock \
    git@github.com:ceph/dmclock.git K_way_heap --squash

# add other subtree pull commands here...
