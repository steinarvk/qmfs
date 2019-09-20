load helpers

@test "can read and write files and survive persistence" {
  echo "hello world" > "${Q}/entities/all/myentity/attribute"
  echo "hi there" > "${Q}/entities/all/anotherentity/anotherattribute"

  restart_qmfs

  md5sum ${Q}/entities/all/myentity/attribute >&3
  [ "hi there" = "$(cat ${Q}/entities/all/anotherentity/anotherattribute)" ]
  [ "hello world" = "$(cat ${Q}/entities/all/myentity/attribute)" ]
}

@test "can create an empty file by touching and survive persistence" {
  touch ${Q}/entities/all/e/a

  restart_qmfs

  checksum=$(md5sum ${Q}/entities/all/e/a | cut -d' ' -f1)
  [ "${checksum}" = "d41d8cd98f00b204e9800998ecf8427e" ]
}

@test "can create a file with a known string and survive persistence" {
  echo -n "hello world" > "${Q}/entities/all/e/known"

  restart_qmfs

  checksum=$(md5sum ${Q}/entities/all/e/known | cut -d' ' -f1)
  [ "${checksum}" = "5eb63bbbe01eeed093cb22bb8f5acdc3" ]
}

@test "can write binary data starting with a null byte without getting confused and survive persistence" {
  echo 00ff01fe02fd03fc04fb05fa | xxd --reverse --plain > ${Q}/entities/all/e/confuse
  echo -n "hello world" > "${Q}/entities/all/e/confuse"

  restart_qmfs

  checksum=$(md5sum ${Q}/entities/all/e/confuse | cut -d' ' -f1)
  [ "${checksum}" = "5eb63bbbe01eeed093cb22bb8f5acdc3" ]
}

@test "can read length of a file and survive persistence" {
  echo -n "hello world!" > "${Q}/entities/all/myentity/attribute"

  restart_qmfs

  len=$(stat --printf="%s" "${Q}/entities/all/myentity/attribute")
  [ "${len}" = "12" ]
}

@test "can read length of an empty file and survive persistence" {
  touch "${Q}/entities/all/myentity/attribute"

  restart_qmfs

  len=$(stat --printf="%s" "${Q}/entities/all/myentity/attribute")
  [ "${len}" = "0" ]
}

@test "can create a file by appending and survive persistence" {
  echo -n hello1 >> "${Q}/entities/all/myentity/attribute"
  echo -n hello2 >> "${Q}/entities/all/myentity/attribute"
  
  restart_qmfs

  [ "$(cat ${Q}/entities/all/myentity/attribute)" = "hello1hello2" ]
}

@test "can overwrite a file and survive persistence" {
  echo -n hello1 > "${Q}/entities/all/myentity/attribute"
  echo -n hello2 > "${Q}/entities/all/myentity/attribute"

  restart_qmfs

  [ "$(cat ${Q}/entities/all/myentity/attribute)" = "hello2" ]
}

@test "can create a file with whitespace prefix and survive persistence" {
  echo -n "  hello" > "${Q}/entities/all/myentity/attribute"

  restart_qmfs

  [ "$(cat ${Q}/entities/all/myentity/attribute)" = "  hello" ]
  [ "$(cat ${Q}/entities/all/myentity/attribute)" != "hello" ]
}

@test "can create a file with whitespace suffix and survive persistence" {
  echo -n "hello  " > "${Q}/entities/all/myentity/attribute"

  restart_qmfs

  [ "$(cat ${Q}/entities/all/myentity/attribute)" = "hello  " ]
  [ "$(cat ${Q}/entities/all/myentity/attribute)" != "hello" ]
}

@test "can create a file with surrounding whitespace and survive persistence" {
  echo -n "  hello   " > "${Q}/entities/all/myentity/attribute"

  restart_qmfs

  [ "$(cat ${Q}/entities/all/myentity/attribute)" = "  hello   " ]
  [ "$(cat ${Q}/entities/all/myentity/attribute)" != "   hello  " ]
  [ "$(cat ${Q}/entities/all/myentity/attribute)" != "hello" ]
}

@test "can deal with normal binary data and survive persistence" {
  echo -n "$(seq 1000)" | gzip - > ${Q}/entities/all/e/data.gz

  restart_qmfs

  data=$(cat ${Q}/entities/all/e/data.gz | gzip -d -)
  [ "$(seq 1000)" = "${data}" ]
}
