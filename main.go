package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"io/ioutil"

	"os/user"
	"path"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	bucketsync "github.com/juntaki/bucketsync/lib"
	"github.com/urfave/cli"
	yaml "gopkg.in/yaml.v2"
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
		{
			Name:   "config",
			Usage:  "Unmount bucketsync filesystem",
			Action: config,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "bucket",
					Value: "",
					Usage: "S3 bucket name",
				},
				cli.StringFlag{
					Name:  "region",
					Value: "",
					Usage: "S3 region name",
				},
				cli.StringFlag{
					Name:  "accesskey",
					Value: "",
					Usage: "S3 access key",
				},
				cli.StringFlag{
					Name:  "secretkey",
					Value: "",
					Usage: "S3 secret access key",
				},
				cli.StringFlag{
					Name:  "password",
					Value: "",
					Usage: "password for data encryption",
				},
				cli.StringFlag{
					Name:  "logging",
					Value: "production",
					Usage: "logging mode",
				},
			},
		},
	}

	app.Run(os.Args)
}

func readConfig() (*bucketsync.Config, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, err
	}
	configYAML, err := ioutil.ReadFile(path.Join(usr.HomeDir, ".bucketsync.yml"))
	if err != nil {
		return nil, err
	}

	config := &bucketsync.Config{}
	err = yaml.Unmarshal(configYAML, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func config(cli *cli.Context) error {
	config, err := readConfig()
	if err != nil {
		config = &bucketsync.Config{}
	}
	if cli.String("bucket") != "" {
		config.Bucket = cli.String("bucket")
	}
	if cli.String("region") != "" {
		config.Region = cli.String("region")
	}
	if cli.String("accesskey") != "" {
		config.AccessKey = cli.String("accesskey")
	}
	if cli.String("secretkey") != "" {
		config.Bucket = cli.String("secretkey")
	}
	if cli.String("password") != "" {
		config.Password = cli.String("password") // TODO: hash
	}
	if cli.String("logging") != "" {
		config.Logging = cli.String("logging")
	}
	// TODO: check logging mode
	configYAML, err := yaml.Marshal(config)
	if err != nil {
		return err
	}
	usr, err := user.Current()
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(path.Join(usr.HomeDir, ".bucketsync.yml"), configYAML, 0600)
	if err != nil {
		return err
	}
	return nil
}

func mount(cli *cli.Context) error {
	config, err := readConfig()
	if err != nil {
		return err
	}
	if cli.String("dir") == "" {
		fmt.Println("Specify mount point")
		os.Exit(1)
	}
	fs := bucketsync.NewFileSystem(config)
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
