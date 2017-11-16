package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"

	"io/ioutil"

	"os/user"
	"path"

	"strconv"

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
				cli.BoolFlag{
					Name:   "daemon",
					Hidden: true,
				},
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
				pidStr, err := ioutil.ReadFile(configDir("pidfile"))
				if err != nil {
					return err
				}

				pid, err := strconv.Atoi(string(pidStr))
				if err != nil {
					return err
				}

				process, err := os.FindProcess(pid)
				if err != nil {
					return err
				}
				process.Signal(os.Interrupt)
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

func configDir(filename string) string {
	usr, err := user.Current()
	if err != nil {
		return filename
	}
	configPath := path.Join(usr.HomeDir, ".bucketsync")

	file, err := os.Open(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(configPath, 0700)
			if err != nil {
				return filename
			}
			file, err = os.Open(configPath)
			if err != nil {
				return filename
			}
		} else {
			return filename
		}
	}

	stat, err := file.Stat()
	if err != nil || !stat.IsDir() {
		return filename
	}

	return path.Join(configPath, filename)
}

func readConfig() (*bucketsync.Config, error) {
	configYAML, err := ioutil.ReadFile(configDir("config.yml"))
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
		config = &bucketsync.Config{
			Encryption:  true,
			Compression: true,
		}
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

	// advance setting
	if config.LogOutputPath == "" {
		config.LogOutputPath = configDir("bucketsync.log")
	}
	if config.CacheSize == 0 {
		config.CacheSize = 1024
	}
	if config.ExtentSize == 0 {
		config.ExtentSize = 1024 * 64
	}

	// TODO: check logging mode
	configYAML, err := yaml.Marshal(config)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(path.Join(configDir("config.yml")), configYAML, 0600)
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

	// Exec daemon
	if !cli.Bool("daemon") {
		args := append(os.Args[1:len(os.Args)], "--daemon")
		cmd := exec.Command(os.Args[0], args...)
		err := cmd.Start()
		if err != nil {
			return err
		}
		ioutil.WriteFile(configDir("pidfile"), []byte(strconv.Itoa(cmd.Process.Pid)), 0600)
		os.Exit(0)
	}

	fs := bucketsync.NewFileSystem(config)
	fs.SetDebug(true)

	s, _, err := nodefs.MountRoot(cli.String("dir"), fs.Root(), nil)
	if err != nil {
		panic(err)
	}

	// unmount
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for {
			<-c
			err := s.Unmount()
			if err == nil {
				break
			}
			log.Print("unmount failed: ", err)
		}
	}()

	s.Serve()
	return nil
}
