package main

import (
  "github.com/steinarvk/orclib/lib/orcmain"

  "github.com/steinarvk/qmfs/cmd"
)

func init() {
	orcmain.Init("qmfs", cmd.Root)
}

func main() {
	orcmain.Main(cmd.Root)
}
