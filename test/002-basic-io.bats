load helpers

@test "can read and write files" {
  echo "hello world" > "${Q}/entities/all/myentity/attribute"
  echo "hi there" > "${Q}/entities/all/anotherentity/anotherattribute"
  md5sum ${Q}/entities/all/myentity/attribute >&3
  [ "hi there" = "$(cat ${Q}/entities/all/anotherentity/anotherattribute)" ]
  [ "hello world" = "$(cat ${Q}/entities/all/myentity/attribute)" ]
}

@test "can create an empty file by touching" {
  touch ${Q}/entities/all/e/a
  checksum=$(md5sum ${Q}/entities/all/e/a | cut -d' ' -f1)
  [ "${checksum}" = "d41d8cd98f00b204e9800998ecf8427e" ]
}

@test "can create a file with a known string" {
  echo -n "hello world" > "${Q}/entities/all/e/known"
  checksum=$(md5sum ${Q}/entities/all/e/known | cut -d' ' -f1)
  [ "${checksum}" = "5eb63bbbe01eeed093cb22bb8f5acdc3" ]
}

@test "can read length of a file" {
  echo -n "hello world!" > "${Q}/entities/all/myentity/attribute"
  len=$(stat --printf="%s" "${Q}/entities/all/myentity/attribute")
  [ "${len}" = "12" ]
}

@test "can read length of an empty file" {
  touch "${Q}/entities/all/myentity/attribute"
  len=$(stat --printf="%s" "${Q}/entities/all/myentity/attribute")
  [ "${len}" = "0" ]
}

@test "can create a file by appending" {
  echo -n hello1 >> "${Q}/entities/all/myentity/attribute"
  echo -n hello2 >> "${Q}/entities/all/myentity/attribute"
  [ "$(cat ${Q}/entities/all/myentity/attribute)" = "hello1hello2" ]
}

@test "can overwrite a file" {
  echo -n hello1 > "${Q}/entities/all/myentity/attribute"
  echo -n hello2 > "${Q}/entities/all/myentity/attribute"
  [ "$(cat ${Q}/entities/all/myentity/attribute)" = "hello2" ]
}

@test "can create a file with whitespace prefix" {
  echo -n "  hello" > "${Q}/entities/all/myentity/attribute"
  [ "$(cat ${Q}/entities/all/myentity/attribute)" = "  hello" ]
  [ "$(cat ${Q}/entities/all/myentity/attribute)" != "hello" ]
}

@test "can create a file with whitespace suffix" {
  echo -n "hello  " > "${Q}/entities/all/myentity/attribute"
  [ "$(cat ${Q}/entities/all/myentity/attribute)" = "hello  " ]
  [ "$(cat ${Q}/entities/all/myentity/attribute)" != "hello" ]
}

@test "can create a file with surrounding whitespace" {
  echo -n "  hello   " > "${Q}/entities/all/myentity/attribute"
  [ "$(cat ${Q}/entities/all/myentity/attribute)" = "  hello   " ]
  [ "$(cat ${Q}/entities/all/myentity/attribute)" != "   hello  " ]
  [ "$(cat ${Q}/entities/all/myentity/attribute)" != "hello" ]
}

@test "can deal with normal binary data" {
  echo -n "$(seq 1000)" | gzip - > ${Q}/entities/all/e/data.gz
  data=$(cat ${Q}/entities/all/e/data.gz | gzip -d -)
  [ "$(seq 1000)" = "${data}" ]
}

@test "can copy a file" {
  echo "hello" > "${Q}/entities/all/myentity/attribute"
  cp ${Q}/entities/all/myentity/attribute{,.copy}
  [ "$(cat ${Q}/entities/all/myentity/attribute.copy)" = "hello" ]
}

@test "can copy a file between entities" {
  echo "hello" > "${Q}/entities/all/myentity/attribute"
  cp ${Q}/entities/all/myentity{,.copy}/attribute
  [ "$(cat ${Q}/entities/all/myentity.copy/attribute)" = "hello" ]
}

@test "can check for the existence of a nonexistent file" {
  [ ! -f ${Q}/entities/all/foo/bar ]
}

@test "can check for the existence of a nonexistent file, then create it" {
  [ ! -f ${Q}/entities/all/foo/bar ]
  touch ${Q}/entities/all/foo/bar
  [ -f ${Q}/entities/all/foo/bar ]
}

@test "can delete a file" {
  touch ${Q}/entities/all/foo/bar
  [ -f ${Q}/entities/all/foo/bar ]
  rm ${Q}/entities/all/foo/bar
  [ ! -f ${Q}/entities/all/foo/bar ]
}
