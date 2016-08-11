BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA reader_schema
        AUTHORIZATION postgres;

SELECT  dblink_connect('sys_syn_data', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)));
SELECT dblink_exec('sys_syn_data', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('group');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('group');

SELECT sys_syn_dblink.reading_table_add (
        schema          => 'reader_schema',
        in_table_id     => 'test_data',
        out_group_id    => 'group',
        in_group_id     => 'group',
        dblink_connname => 'sys_syn_data');


SELECT * FROM reader_schema.test_data_group_claim();

SELECT * FROM reader_schema.test_data_group_pull();

SELECT * FROM reader_schema.test_data_group_process();

SELECT * FROM reader_schema.test_data_group_push_status();


SELECT dblink_exec('sys_syn_data', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_data');

ROLLBACK;
