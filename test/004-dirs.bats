load helpers

@test "can create a directory" {
  [ ! -d ${Q}/entities/all/e/dir ]
  mkdir ${Q}/entities/all/e/dir
  [ -d ${Q}/entities/all/e/dir ]
}

@test "can create and delete a directory" {
  mkdir ${Q}/entities/all/e/dir
  rm -r ${Q}/entities/all/e/dir
}

@test "can create a file within a directory" {
  mkdir ${Q}/entities/all/e/dir
  echo hello > ${Q}/entities/all/e/dir/world
  [ "$(cat ${Q}/entities/all/e/dir/world)" = "hello" ]
}

@test "cannot delete a directory as if it were a normal file" {
  mkdir ${Q}/entities/all/e/dir
  run rm ${Q}/entities/all/e/dir
  [ $status -ne 0 ]
}

@test "can delete a normal file" {
  touch ${Q}/entities/all/e/dir
  run rm ${Q}/entities/all/e/dir
  [ $status -eq 0 ]
}

@test "can create a directory structure with mkdir -p" {
  mkdir -p ${Q}/entities/all/e/dir1/dir2/dir3/dir4
  mkdir -p ${Q}/entities/all/e/dir1/dir2/dir5/dir6
  [ -d ${Q}/entities/all/e/dir1 ]
  [ -d ${Q}/entities/all/e/dir1/dir2 ]
  [ -d ${Q}/entities/all/e/dir1/dir2/dir3 ]
  [ -d ${Q}/entities/all/e/dir1/dir2/dir3/dir4 ]
  [ -d ${Q}/entities/all/e/dir1/dir2/dir5 ]
  [ -d ${Q}/entities/all/e/dir1/dir2/dir5/dir6 ]
}

@test "can create a directory and survive persistence" {
  mkdir ${Q}/entities/all/e/dir

  [ -d ${Q}/entities/all/e/dir ]

  restart_qmfs

  [ -d ${Q}/entities/all/e/dir ]
}

@test "can create a directory, delete it, and replace it with a file" {
  mkdir ${Q}/entities/all/e/hm
  
  [ -d ${Q}/entities/all/e/hm ]
  [ ! -f ${Q}/entities/all/e/hm ]

  rm -r ${Q}/entities/all/e/hm

  [ ! -d ${Q}/entities/all/e/hm ]
  [ ! -f ${Q}/entities/all/e/hm ]


  echo hello > ${Q}/entities/all/e/hm

  [ ! -d ${Q}/entities/all/e/hm ]
  [ -f ${Q}/entities/all/e/hm ]
}

@test "can create a directory structure with mkdir -p and traverse it with find" {
  mkdir -p ${Q}/entities/all/e/dir1/dir2/dir3/dir4
  mkdir -p ${Q}/entities/all/e/dir1/dir2/dir5/dir6
  output=$(find ${Q}/entities/all/e/dir1 | sed "sX.*entities/allXXg" | wc -l)
  [ "$output" = "6" ]
}

@test "cannot create a directory twice" {
  run mkdir ${Q}/entities/all/e/dir1
  [ $status -eq 0 ]
  run mkdir ${Q}/entities/all/e/dir1
  [ $status -eq 1 ]
}

@test "can overwrite a file with a file" {
  run touch ${Q}/entities/all/e/file1
  [ $status -eq 0 ]
  run bash -c "echo contents > ${Q}/entities/all/e/file1"
  [ $status -eq 0 ]
}

@test "cannot overwrite a directory with a file" {
  run mkdir ${Q}/entities/all/e/dir1
  [ $status -eq 0 ]
  run bash -c "echo contents > ${Q}/entities/all/e/dir1"
  [ $status -eq 1 ]
}

@test "cannot create a directory in place of an existing file" {
  run touch ${Q}/entities/all/e/dir1
  [ $status -eq 0 ]
  run mkdir ${Q}/entities/all/e/dir1
  [ $status -eq 1 ]
}

@test "can create a file in place of a deleted directory" {
  run mkdir ${Q}/entities/all/e/dir1
  [ $status -eq 0 ]
  rm -r ${Q}/entities/all/e/dir1
  run bash -c "echo contents > ${Q}/entities/all/e/dir1"
  [ $status -eq 0 ]
}

@test "can create a directory in place of a deleted file" {
  run touch ${Q}/entities/all/e/dir1
  [ $status -eq 0 ]
  rm ${Q}/entities/all/e/dir1
  run mkdir ${Q}/entities/all/e/dir1
  [ $status -eq 0 ]
}
