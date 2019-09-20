load helpers

@test "/service/ exists" {
  [ -d "${Q}/service" ]
}

@test "/service/version.json exists" {
  [ -f "${Q}/service/version.json" ]
}

@test "/service/nonexistent does not exist" {
  [ ! -f "${Q}/service/nonexistent" ]
}

@test "/service/pid is persistent" {
  first=$(cat "${Q}/service/pid")
  second=$(cat "${Q}/service/pid")
  [ "$first" = "$second" ]
}

@test "/service/pid changes on restart" {
  first=$(cat "${Q}/service/pid")
  restart_qmfs
  second=$(cat "${Q}/service/pid")
  [ "$first" != "$second" ]
}

