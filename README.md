# bucketsync

bucketsync is S3 backed FUSE Filesystem written in Golang. it can mount Amazon S3 as filesystem.

## Features

* Block-level deduplication
* Client side encryption

## How to use

~~~
export AWS_BUCKET_NAME=<bucket_name>
export AWS_BUCKET_REGION=<reginon e.g. ap-northeast-1>
export AWS_ACCESS_KEY_ID=<AWS access key>
export AWS_SECRET_ACCESS_KEY=<AWS secret key>

./bucketsync -m /path/to/mountpoint
~~~

## TODO

- [ ] Performance improvement
  - [ ] Client cache
  - [ ] Reduce request
- [ ] Server side garbage collection
- [ ] Access control
- [ ] Stat FS / Quota
- [ ] Multi clients support (locking)
