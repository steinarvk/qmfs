load helpers

setup_simpsons() {
  echo Lisa > "${Q}/entities/all/lisa/firstname"
  echo Simpson > "${Q}/entities/all/lisa/lastname"
  echo 8 > "${Q}/entities/all/lisa/age"
  echo female > "${Q}/entities/all/lisa/sex"

  echo Bart > "${Q}/entities/all/bart/firstname"
  echo Simpson > "${Q}/entities/all/bart/lastname"
  echo 10 > "${Q}/entities/all/bart/age"
  echo male > "${Q}/entities/all/bart/sex"
  echo Stampy > "${Q}/entities/all/bart/elephant"

  echo Homer > "${Q}/entities/all/homer/firstname"
  echo Simpson > "${Q}/entities/all/homer/lastname"
  echo 39 > "${Q}/entities/all/homer/age"
  echo male > "${Q}/entities/all/homer/sex"

  echo Marge > "${Q}/entities/all/marge/firstname"
  echo Simpson > "${Q}/entities/all/marge/lastname"
  echo 36 > "${Q}/entities/all/marge/age"
  echo female > "${Q}/entities/all/marge/sex"

  echo Maggie > "${Q}/entities/all/maggie/firstname"
  echo Simpson > "${Q}/entities/all/maggie/lastname"
  echo 1 > "${Q}/entities/all/maggie/age"
  echo female > "${Q}/entities/all/maggie/sex"

  echo Ned > "${Q}/entities/all/flanders/firstname"
  echo Flanders > "${Q}/entities/all/flanders/lastname"
  echo 60 > "${Q}/entities/all/flanders/age"
  echo male > "${Q}/entities/all/flanders/sex"
  echo Christian > "${Q}/entities/all/flanders/religion"

  echo Itchy > "${Q}/entities/all/itchy/firstname"
  echo male > "${Q}/entities/all/itchy/sex"
  touch "${Q}/entities/all/itchy/fictional"

  echo Scratchy > "${Q}/entities/all/scratchy/firstname"
  echo male > "${Q}/entities/all/scratchy/sex"
  touch "${Q}/entities/all/scratchy/fictional"
}

@test "can enumerate all entities" {
  setup_simpsons
  [ "$(ls ${Q}/entities/all | wc -l | tr -d '[:space:]')" = "8" ]
}

@test "can query by existence of a file" {
  setup_simpsons
  [ "$(ls ${Q}/query/religion/all | wc -l | tr -d '[:space:]')" = "1" ]
}

@test "can query by nonexistence of a file" {
  setup_simpsons
  [ "$(ls ${Q}/query/-religion/all | wc -l | tr -d '[:space:]')" = "7" ]
}

@test "can query by contents of a file" {
  setup_simpsons
  [ "$(ls ${Q}/query/sex=male/all | wc -l | tr -d '[:space:]')" = "5" ]
}

@test "can query by existence of one file, contents of another" {
  setup_simpsons
  [ "$(ls ${Q}/query/fictional,firstname=Scratchy/all | wc -l | tr -d '[:space:]')" = "1" ]
}

@test "can perform a query that yields no results" {
  setup_simpsons
  [ "$(ls ${Q}/query/fictional,firstname=Scrotchy/all | wc -l | tr -d '[:space:]')" = "0" ]
}

@test "can query by nonexistence of one file, contents of another" {
  setup_simpsons
  [ "$(ls ${Q}/query/-fictional,sex=male/all | wc -l | tr -d '[:space:]')" = "3" ]
}

@test "can query by the contents of two files" {
  setup_simpsons
  [ "$(ls ${Q}/query/firstname=Homer,lastname=Simpson/all | wc -l | tr -d '[:space:]')" = "1" ]
}

@test "can use link to entities from query to refer to other files" {
  setup_simpsons
  [ "$(echo $(cat ${Q}/query/lastname=Simpson/all/*/firstname | sort))" = "Bart Homer Lisa Maggie Marge" ]
}

@test "will not enumerate deleted entities" {
  setup_simpsons
  rm ${Q}/entities/all/itchy/*
  [ "$(ls ${Q}/entities/all | wc -l | tr -d '[:space:]')" = "7" ]
}

@test "will not return deleted entities from queries" {
  setup_simpsons
  rm ${Q}/entities/all/itchy/*
  [ "$(ls ${Q}/query/fictional/all | wc -l | tr -d '[:space:]')" = "1" ]
  touch ${Q}/entities/all/itchy/fictional
  [ "$(ls ${Q}/query/fictional/all | wc -l | tr -d '[:space:]')" = "2" ]
}

@test "will not query by deleted attributes" {
  setup_simpsons
  rm ${Q}/entities/all/lisa/lastname
  [ "$(echo $(cat ${Q}/query/lastname=Simpson/all/*/firstname | sort))" = "Bart Homer Maggie Marge" ]
}
