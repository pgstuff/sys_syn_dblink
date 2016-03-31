BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA reader_data
        AUTHORIZATION postgres;

SELECT dblink_connect('sys_syn_test', 'dbname=contrib_regression');
SELECT dblink_exec('sys_syn_test', 'BEGIN');

/*INSERT INTO sys_syn_dblink.put_column_transforms(
        rule_group_id,  priority,       data_type_like,
        in_table_id_like,       out_group_id_like,      in_group_id_like,
        schema_like,    in_column_type,                         column_name_like,
        new_data_type,  new_in_column_type,                     new_column_name,
        expression,
        create_put_columns,
        omit,   final_ids,                      final_rule)
VALUES (NULL,           100,            NULL,
        'test_table_array',     NULL,                   NULL,
        NULL,           NULL,                                   'test_table_array_updated',
        NULL,           NULL,                                   NULL,
        NULL,
        NULL,
        NULL,   ARRAY[]::TEXT[],                FALSE);*/

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('in');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('out');

SELECT sys_syn_dblink.reading_table_add (
        schema          => 'reader_data',
        in_table_id     => 'test_table_array',
        out_group_id    => 'out',
        in_group_id     => 'in',
        dblink_connname => 'sys_syn_test');


SELECT * FROM reader_data.test_table_array_out_claim();

SELECT * FROM reader_data.test_table_array_out_pull();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, key, attributes, no_diff
FROM    reader_data.test_table_array_out_reading
ORDER BY key, attributes;

SELECT * FROM reader_data.test_table_array_out_process();

SELECT  test_table_array_key, test_table_array_updated, test_table_array_text
FROM    reader_data.test_table_array_out
ORDER BY test_table_array_key, test_table_array_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    reader_data.test_table_array_out_read
ORDER BY reading_key;

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, key, attributes, no_diff
FROM    reader_data.test_table_array_out_hold
ORDER BY key, attributes;

SELECT * FROM reader_data.test_table_array_out_push_status();


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
