package cmd

import (
	"github.com/spf13/cobra"
)

var Root = &cobra.Command{
	Use:   "qmfs",
	Short: "A metadata database with a fuse-based filesystem interface",
}
