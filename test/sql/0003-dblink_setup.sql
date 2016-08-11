BEGIN;

CREATE EXTENSION sys_syn;

CREATE EXTENSION dblink;

CREATE EXTENSION sys_syn_dblink;

SELECT  dblink_connect('sys_syn_dblink_test', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)));

SELECT dblink_disconnect('sys_syn_dblink_test');

ROLLBACK;
