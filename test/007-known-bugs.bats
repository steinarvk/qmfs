load helpers

@test "can create an empty file by touching and survive persistence with no pausing" {
  skip

  # Known issue:
  #   "Releasing" a file (i.e. closing the last handle) is not instant.
  #   If you close the last handle and immediately after kill the
  #   qmfs server, the code that's supposed to trigger on the
  #   release may not run.
  #   As a write-minimizing trick, we perform certain flushes at this
  #   time. As such, writing and then immediately killing the qmfs
  #   server ungracefully may not stick.
  
  touch ${Q}/entities/all/e/a

  instantly_and_ungracefully_restart_qmfs

  checksum=$(md5sum ${Q}/entities/all/e/a | cut -d' ' -f1)
  [ "${checksum}" = "d41d8cd98f00b204e9800998ecf8427e" ]
}
