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
        in_table_id     => 'test_table',
        out_group_id    => 'out',
        put_group_id    => 'put',
        dblink_connname => 'sys_syn_test');


SELECT * FROM processor_data.test_table_claim_1();

SELECT * FROM processor_data.test_table_pull_1();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_processing_1
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_process_1();

SELECT  *
FROM    test_table
ORDER BY test_table_id, test_table_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_processed_1
ORDER BY id;


SELECT * FROM processor_data.test_table_push_status_1();

SELECT * FROM dblink('sys_syn_test', $$SELECT user_data.test_table_out_processed_1()$$) AS test_table_processed(result text);

SELECT * FROM dblink_exec('sys_syn_test', $$UPDATE user_data.test_table SET test_table_text = 'test_data-b 2' WHERE test_table_id = 2$$);

SELECT * FROM dblink_exec('sys_syn_test', $$DELETE FROM user_data.test_table WHERE test_table_id = 1$$);

SELECT * FROM dblink_exec('sys_syn_test', $$UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1$$);

SELECT * FROM dblink('sys_syn_test', $$SELECT user_data.test_table_pull(FALSE)$$) AS test_table_pull(result text);

SELECT * FROM dblink('sys_syn_test', $$SELECT user_data.test_table_out_move_1()$$) AS test_table_move(result text);

SELECT * FROM processor_data.test_table_claim_1();

SELECT * FROM processor_data.test_table_pull_1();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_processing_1
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_process_1();

SELECT  *
FROM    test_table
ORDER BY test_table_id, test_table_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_processed_1
ORDER BY id;


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
