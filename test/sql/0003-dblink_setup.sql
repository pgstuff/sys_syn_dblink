BEGIN;

CREATE EXTENSION sys_syn;

CREATE EXTENSION dblink;

CREATE EXTENSION sys_syn_dblink;

SELECT dblink_connect('sys_syn_dblink_test', 'dbname=contrib_regression');

SELECT dblink_disconnect('sys_syn_dblink_test');

ROLLBACK;
