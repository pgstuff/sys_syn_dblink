BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA processor_schema
        AUTHORIZATION postgres;

SELECT  dblink_connect('sys_syn_data', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)));
SELECT dblink_exec('sys_syn_data', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('sys_syn_data', 'group');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('sys_syn_data', 'group');
INSERT INTO sys_syn_dblink.put_groups_def VALUES ('put_group');

SELECT sys_syn_dblink.processing_table_create (
        proc_schema     => 'processor_schema',
        in_table_id     => 'test_data',
        out_group_id    => 'group',
        put_group_id    => 'put_group',
        dblink_connname => 'sys_syn_data');


SELECT * FROM processor_schema.test_data_group_claim();

SELECT * FROM processor_schema.test_data_group_pull();

SELECT * FROM processor_schema.test_data_group_process();

SELECT * FROM processor_schema.test_data_group_push_status();


SELECT dblink_exec('sys_syn_data', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_data');

ROLLBACK;
