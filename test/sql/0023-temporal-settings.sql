BEGIN;

CREATE EXTENSION sys_syn_dblink;
CREATE EXTENSION temporal_tables;
CREATE EXTENSION btree_gist;

INSERT INTO sys_syn_dblink.put_table_transforms(
        rule_group_id,          priority,       final_ids,              proc_table_id_like,
        table_settings)
VALUES (NULL,                   25,             '{}',                   'test_table_array',
        $$
        sys_syn.temporal.active_table_name      => %1_active,
        sys_syn.temporal.history_table_name     => %1_inactive,
        sys_syn.temporal.range_1.column_name    => trans_period
        $$);

CREATE SCHEMA processor_data
        AUTHORIZATION postgres;

CREATE SCHEMA put_data
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
        in_table_id     => 'test_table_array',
        out_group_id    => 'out',
        put_group_id    => 'put',
        put_schema      => 'put_data',
        table_type_id   => 'sys_syn-temporal',
        dblink_connname => 'sys_syn_test');


SELECT * FROM processor_data.test_table_array_claim_1();

SELECT * FROM processor_data.test_table_array_pull_1();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_array_processing_1
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_array_process_1();

SELECT  test_table_array_id, trans_period, test_table_array_text
FROM    put_data.test_table_array_active
ORDER BY test_table_array_id, test_table_array_text;

SELECT  test_table_array_id, trans_period, test_table_array_text
FROM    put_data.test_table_array_inactive
ORDER BY test_table_array_id, test_table_array_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_array_processed_1
ORDER BY id;

SELECT * FROM processor_data.test_table_array_push_status_1();

SELECT * FROM dblink('sys_syn_test', $$SELECT user_data.test_table_array_out_processed_1()$$) AS test_table_array_processed(result text);


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
