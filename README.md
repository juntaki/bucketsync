# bucketsync

bucketsync is S3 backed FUSE Filesystem written in Golang. it can mount Amazon S3 as filesystem.

## Features

* Block-level deduplication
* Client side encryption

## How to use

Install

~~~
go get -u -v github.com/juntaki/bucketsync
~~~

Run

~~~
bucketsync config --bucket <Bucket name> \
                  --region <Region, e.g. ap-northeast-1> \
                  --accesskey <AWS access key> \
                  --secretkey <AWS secret key> \
                  --password <Password for data encryption>

bucketsync mount --dir /path/to/mountpoint
~~~

## TODO

- [ ] Performance improvement
  - [ ] Client cache
  - [ ] Reduce request
- [ ] Server side garbage collection
- [ ] Access control
- [ ] Stat FS / Quota
- [ ] Multi clients support (locking)
