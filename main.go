package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	bucketsync "github.com/juntaki/bucketsync/lib"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "bucketsync"
	app.Usage = "S3 as Filesystem"
	app.Version = "0.0.1"

	app.Commands = []cli.Command{
		{
			Name:   "mount",
			Usage:  "Mount S3 as filesystem",
			Action: mount,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "dir",
					Value: "",
					Usage: "Specifies the mount point path",
				},
			},
		},
		{
			Name:    "unmount",
			Aliases: []string{"umount"},
			Usage:   "Unmount bucketsync filesystem",
			Action: func(c *cli.Context) error {
				fmt.Println("unmount", c.Args().First())
				return nil
			},
		},
	}

	app.Run(os.Args)
}

func mount(cli *cli.Context) error {
	if cli.String("dir") == "" {
		fmt.Println("Specify mount point")
		os.Exit(1)
	}
	fs := bucketsync.NewFileSystem()
	fs.SetDebug(true)

	s, _, err := nodefs.MountRoot(cli.String("dir"), fs.Root(), nil)
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
	return nil
}
