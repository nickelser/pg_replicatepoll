# pg_replicatepoll

Replicate a Postgres table with no dependencies, using the magic of polling.

## Install

pg_replicatepoll is a command line tool. To install, run:

```sh
go install github.com/nickelser/pg_replicatepoll
```

This will give you the `pg_replicatepoll` command, assuming your Go paths are setup correctly.

## Usage

```sh
pg_replicatepoll --source=postgres://user:pass@db.freepostgresservers.com/sourcedb --target=postgres://user:pass@db2.freepostgresservers.com/targetdb --table=users --tscol=updated_at --limit=500
```

TODO: more examples!

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/nickelser/pg_replicatepoll/issues)
- Fix bugs and [submit pull requests](https://github.com/nickelser/pg_replicatepoll/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features
