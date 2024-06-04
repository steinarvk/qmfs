# qmfs (quick metadata filesystem)

qmfs is a program that exposes a fuse-based filesystem
interface for editing and performing low-level queries
on metadata. The metadata itself is stored in a SQLite
database.

Essentially it provides a convenience layer through
which it becomes pretty easy to use a SQLite database
in a simple script or even a shell alias.

The database has a fixed but generic schema, oriented
around "entities" that have attributes ("files").
In the filesystem layer, entities are represented by
directories containing files.

Several forms of queries can be performed through
the filesystem by looking up a magic directory
under the "query" directory. For instance,
"query/foo,bar/list" will be a file containing a list
of all the entities that have both a file called
"foo" and a file called "bar". "query/foo=42,-bar/all" 
will be a directory containing symlinks to each of
the entities where there is a file called "foo"
with contents equal to "42" and _no_ file called "bar".

## Example

```
$ mkdir /tmp/foo
$ qmfs serve --mountpoint /tmp/foo --localdb db.sqlite3
$ echo foo > /tmp/foo/entities/all/ent1/attribute
$ echo bar > /tmp/foo/entities/all/ent2/attribute
$ echo baz > /tmp/foo/entities/all/ent3/attribute
$ echo quux > /tmp/foo/entities/all/ent4/attribute
$ echo foo > /tmp/foo/entities/all/ent5/attribute
$ echo foo > /tmp/foo/entities/all/ent6/attribute2
$ touch /tmp/foo/entities/all/ent7/attribute{,2}
$ cat /tmp/foo/query/attribute=foo/list
/tmp/foo/entities/shard/32/fd/ent1
/tmp/foo/entities/shard/32/cb/ent5
$ cat /tmp/foo/query/attribute,-attribute2,-attribute=foo/list
/tmp/foo/entities/shard/9c/3f/ent2
/tmp/foo/entities/shard/63/a4/ent3
/tmp/foo/entities/shard/db/8e/ent4
```

More examples of what qmfs can do can be seen by
inspecting the BATS tests in the test/ directory.

## Authorship

qmfs was written by me, Steinar V. Kaldager.

This is a hobby project that I wrote primarily for my own
usage several years before publication. It is no longer
actively maintained, you probably shouldn't use it and,
and you definitely shouldn't expect support for it.
Not affiliated with any employer of mine past or present.

It is released as open source under the MIT license. Feel
free to make use of it however you wish under the terms
of that license.

## License

This project is licensed under the MIT License - see the
LICENSE file for details.
