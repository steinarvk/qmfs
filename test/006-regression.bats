load helpers

@test "can write binary data starting with a null byte without getting confused" {
  echo 00ff01fe02fd03fc04fb05fa | xxd --reverse --plain > ${Q}/entities/all/e/confuse
  echo -n "hello world" > "${Q}/entities/all/e/confuse"
  checksum=$(md5sum ${Q}/entities/all/e/confuse | cut -d' ' -f1)
  [ "${checksum}" = "5eb63bbbe01eeed093cb22bb8f5acdc3" ]
}

@test "can touch a file twice and then overwrite it" {
  touch "${Q}/entities/all/e/touched"
  touch "${Q}/entities/all/e/touched"
  echo -n message > "${Q}/entities/all/e/touched"
  [ "$(cat ${Q}/entities/all/e/touched)" = "message" ]
}
