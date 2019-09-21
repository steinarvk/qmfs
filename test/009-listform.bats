load helpers

@test "can list all entities using /list" {
  for n in $(seq 143); do
    touch "${Q}/entities/all/e${n}/a";
  done
  [ "$(cat "${Q}/entities/list" | wc -l)" = "143" ]
}

@test "can list all entities in a namespace using /list" {
  for n in $(seq 143); do
    touch "${Q}/entities/all/e${n}/a";
    touch "${Q}/namespace/foo/entities/all/e${n}/a";
  done
  touch "${Q}/namespace/foo/entities/all/hello/attrib"
  [ "$(cat "${Q}/entities/list" | wc -l)" = "143" ]
  [ "$(cat "${Q}/namespace/foo/entities/list" | wc -l)" = "144" ]
}

@test "can list the results of a query using /list" {
  touch "${Q}/entities/all/e1/a"
  touch "${Q}/entities/all/e2/a"
  touch "${Q}/entities/all/e2/b"
  touch "${Q}/entities/all/e3/a"
  touch "${Q}/entities/all/e3/c"
  [ "$(cat "${Q}/query/a,c/list" | wc -l)" = "1" ]
}

@test "can list the results of a query in a namespace using /list" {
  touch "${Q}/namespace/foo/entities/all/e1/a"
  touch "${Q}/namespace/foo/entities/all/e2/a"
  touch "${Q}/namespace/foo/entities/all/e2/b"
  touch "${Q}/namespace/foo/entities/all/e3/a"
  touch "${Q}/namespace/foo/entities/all/e3/c"
  [ "$(cat "${Q}/namespace/foo/query/a,c/list" | wc -l)" = "1" ]
}
