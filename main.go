package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	bucketsync "github.com/juntaki/bucketsync/lib"
)

func main() {
	strMountPoint := flag.String("m", "", "Mount point")
	flag.Parse()

	if *strMountPoint == "" {
		fmt.Println("Specify mount point")
		os.Exit(1)
	}

	fs := bucketsync.NewFileSystem()
	fs.SetDebug(true)

	s, _, err := nodefs.MountRoot(*strMountPoint, fs.Root(), nil)
	if err != nil {
		panic(err)
	}

	// Ctrl + C unmount
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT)
	go func(s *fuse.Server) {
		<-c
		fmt.Println("Unmount")
		s.Unmount()
	}(s)

	s.Serve()

}
