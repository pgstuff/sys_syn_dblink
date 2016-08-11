BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA reader_data
        AUTHORIZATION postgres;

SELECT  dblink_connect('sys_syn_test', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)));

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('in');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('out');

SELECT sys_syn_dblink.reading_table_add (
        schema          => 'reader_data',
        in_table_id     => 'test_table',
        out_group_id    => 'out',
        in_group_id     => 'in',
        dblink_connname => 'sys_syn_test');

SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
