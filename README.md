sys_syn_dblink
=======

A sys_syn reader.

To build it, just do this:

    make
    make installcheck
    make install

If you encounter an error such as:

    "Makefile", line 8: Need an operator

You need to use GNU make, which may well be installed on your system as
`gmake`:

    gmake
    gmake install
    gmake installcheck

If you encounter an error such as:

    make: pg_config: Command not found

Be sure that you have `pg_config` installed and in your path. If you used a
package management system such as RPM to install PostgreSQL, be sure that the
`-devel` package is also installed. If necessary tell the build process where
to find it:

    env PG_CONFIG=/path/to/pg_config make && make installcheck && make install

If you encounter an error such as:

    ERROR:  must be owner of database regression

You need to run the test suite using a super user, such as the default
"postgres" super user:

    make installcheck PGUSER=postgres

Once sys_syn_dblink is installed, you can add it to a database. PostgreSQL 9.5.0 or greater
is required. Connect to a database as a super user and run:

    CREATE EXTENSION sys_syn_dblink;

Dependencies
------------
The `sys_syn_dblink` data type has no dependencies other than PostgreSQL.

Copyright and License
---------------------

Copyright (c) 2016 The maintainer's name.

sys_syn_dblink copyright is novated to PostgreSQL Global Development Group.
