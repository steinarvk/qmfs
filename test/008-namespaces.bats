load helpers

@test "can touch a file in non-default namespace without it appearing in default" {
  touch "${Q}/namespace/sidechannel/entities/all/e/a"
  [ ! -f "${Q}/entities/all/e/a" ]
  [ -f "${Q}/namespace/sidechannel/entities/all/e/a" ]
}

@test "can have independent content in different namespaces, including default" {
  echo -n hello > "${Q}/entities/all/e/a"
  echo -n world > "${Q}/namespace/sidechannel/entities/all/e/a"
  echo -n foo > "${Q}/namespace/sidechannel2/entities/all/e/a"
  [ "$(cat ${Q}/entities/all/e/a)" = "hello" ]
  [ "$(cat ${Q}/namespace/sidechannel/entities/all/e/a)" = "world" ]
  [ "$(cat ${Q}/namespace/sidechannel2/entities/all/e/a)" = "foo" ]
}

@test "can remove a file in one namespace without affecting another" {
  echo -n hello > "${Q}/entities/all/e/a"
  echo -n world > "${Q}/namespace/sidechannel/entities/all/e/a"
  rm "${Q}/entities/all/e/a"
  [ ! -f "${Q}/entities/all/e/a" ]
  [ "$(cat ${Q}/namespace/sidechannel/entities/all/e/a)" = "world" ]
}
