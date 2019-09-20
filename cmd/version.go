package cmd

import (
  "fmt"

  "github.com/spf13/cobra"
  "github.com/steinarvk/orc"

  "github.com/steinarvk/orclib/lib/versioninfo"
)

func init() {
  _ = orc.Command(Root, orc.Modules(), cobra.Command{
    Use: "version",
    Short: "Display version information",
  }, func() error {
    for _, line := range versioninfo.VersionInfoLines() {
      fmt.Println(line)
    }
    return nil
  })
}
