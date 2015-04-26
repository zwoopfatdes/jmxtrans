@test "init.d script is created" {
  [ -L /etc/init.d/jmxtrans ]
}

@test "jmxtrans is running" {
  run service jmxtrans status
  [ "$status" -eq 0 ]
}

@test "log directory is created" {
  [ -d /var/log/jmxtrans ]
}

@test "log file exist" {
  [ -f /var/log/jmxtrans/jmxtrans.log ]
}

@test "configuration directory is created" {
  [ -d /etc/jmxtrans ]
}

@test "json configuration directory is created" {
  [ -d /var/lib/jmxtrans ]
}

@test "main jmxtrans dir is created" {
  [ -d /usr/share/jmxtrans ]
}

@test "jmxtrans bin dir is created" {
  [ -d /usr/share/jmxtrans/bin ]
}

@test "jmxtrans lib dir is created" {
  [ -d /usr/share/jmxtrans/lib ]
}
