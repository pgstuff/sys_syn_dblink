BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA processor_data
        AUTHORIZATION postgres;

SELECT  dblink_connect('sys_syn_test', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)) ||
        'port=' || current_setting('port'));
SELECT dblink_exec('sys_syn_test', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('sys_syn_dblink-test', 'in');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('sys_syn_dblink-test', 'out');
INSERT INTO sys_syn_dblink.put_groups_def VALUES ('put');

SELECT sys_syn_dblink.proc_table_create (
        proc_schema     => 'processor_data',
        in_table_id     => 'test_table_part',
        out_group_id    => 'out',
        put_group_id    => 'put',
        dblink_connname => 'sys_syn_test');


SELECT * FROM processor_data.test_table_part_claim_1();
SELECT * FROM processor_data.test_table_part_claim_2();
SELECT * FROM processor_data.test_table_part_claim_3();

SELECT * FROM processor_data.test_table_part_pull_1();
SELECT * FROM processor_data.test_table_part_pull_2();
SELECT * FROM processor_data.test_table_part_pull_3();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_part_processing_1
ORDER BY id, attributes;
SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_part_processing_2
ORDER BY id, attributes;
SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_part_processing_3
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_part_process_1();
SELECT * FROM processor_data.test_table_part_process_2();
SELECT * FROM processor_data.test_table_part_process_3();

SELECT  *
FROM    public.test_table_part
ORDER BY test_table_id, test_table_text;

SELECT * FROM processor_data.test_table_part_push_status_1();
SELECT * FROM processor_data.test_table_part_push_status_2();
SELECT * FROM processor_data.test_table_part_push_status_3();


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
