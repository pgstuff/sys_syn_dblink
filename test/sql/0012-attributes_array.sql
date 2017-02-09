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
        in_table_id     => 'test_table_array',
        out_group_id    => 'out',
        put_group_id    => 'put',
        dblink_connname => 'sys_syn_test');


SELECT * FROM processor_data.test_table_array_out_0_claim();

SELECT * FROM processor_data.test_table_array_out_0_pull();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_array_out_0_processing
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_array_out_0_process();

SELECT  test_table_array_id, test_table_array_updated, test_table_array_text
FROM    processor_data.test_table_array_out
ORDER BY test_table_array_id, test_table_array_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_array_out_0_processed
ORDER BY id;

SELECT * FROM processor_data.test_table_array_out_0_push_status();


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
