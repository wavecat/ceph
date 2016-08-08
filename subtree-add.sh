#!/bin/sh

# dmclock
if [ -d src/dmclock ] ;then
    echo "src/dmclock already exists; skipping"
else
    git subtree add \
	--prefix src/dmclock \
	git@github.com:ceph/dmclock.git K_way_heap --squash
fi

# add other subtree add commands here...
