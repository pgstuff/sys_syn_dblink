BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA processor_data
        AUTHORIZATION postgres;

SELECT  dblink_connect('sys_syn_test', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)));
SELECT dblink_exec('sys_syn_test', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('sys_syn_test', 'in');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('sys_syn_test', 'out');
INSERT INTO sys_syn_dblink.put_groups_def VALUES ('put');

SELECT sys_syn_dblink.processing_table_create (
        proc_schema     => 'processor_data',
        in_table_id     => 'test_table',
        out_group_id    => 'out',
        put_group_id    => 'put',
        dblink_connname => 'sys_syn_test');


SELECT * FROM processor_data.test_table_out_0_claim();


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
