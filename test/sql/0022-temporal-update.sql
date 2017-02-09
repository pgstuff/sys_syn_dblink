BEGIN;

CREATE EXTENSION sys_syn_dblink;
CREATE EXTENSION temporal_tables;
CREATE EXTENSION btree_gist;

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
        put_schema      => 'processor_data',
        table_type_id   => 'sys_syn-temporal',
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

SELECT  test_table_array_id, test_table_array_updated, test_table_array_text
FROM    processor_data.test_table_array_out_history
ORDER BY test_table_array_id, test_table_array_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_array_out_0_processed
ORDER BY id;

SELECT * FROM processor_data.test_table_array_out_0_push_status();

SELECT * FROM dblink('sys_syn_test', $$SELECT user_data.test_table_array_out_processed()$$) AS test_table_array_out_processed(result text);

SELECT * FROM dblink_exec('sys_syn_test', $$UPDATE user_data.test_table_array SET test_table_array_text = 'test_data2 v3' WHERE test_table_array_id = 2$$);

SELECT * FROM dblink_exec('sys_syn_test', $$DELETE FROM user_data.test_table_array WHERE test_table_array_id = 1$$);

SELECT * FROM dblink_exec('sys_syn_test', $$UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1$$);

SELECT * FROM dblink('sys_syn_test', $$SELECT user_data.test_table_array_pull(FALSE)$$) AS test_table_pull(result text);

SELECT * FROM dblink('sys_syn_test', $$SELECT user_data.test_table_array_out_move()$$) AS test_table_array_out_move(result text);

SELECT * FROM processor_data.test_table_array_out_0_claim();

SELECT * FROM processor_data.test_table_array_out_0_pull();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_array_out_0_processing
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_array_out_0_process();

SELECT  test_table_array_id, test_table_array_updated, test_table_array_text
FROM    processor_data.test_table_array_out
ORDER BY test_table_array_id, test_table_array_text;

SELECT  test_table_array_id, test_table_array_updated, test_table_array_text
FROM    processor_data.test_table_array_out_history
ORDER BY test_table_array_id, test_table_array_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_array_out_0_processed
ORDER BY id;


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
