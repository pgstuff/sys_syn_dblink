BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA processor_data
        AUTHORIZATION postgres;

SELECT  dblink_connect('sys_syn_test', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)));
SELECT dblink_exec('sys_syn_test', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('in');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('out');

INSERT INTO sys_syn_dblink.put_column_transforms (
        priority,       data_type_like, out_group_id_like,      in_group_id_like,
        in_table_id_like,       column_name_like,
        variable_name,
        expression,
        variable_exception_traps
) VALUES (
        150,            'text',         'out',                  'in',
        'test_table',           'test_table_text',
        '_var',
        $$'1/(x%2): ' || 1 / (split_part(%1, ' ', 2)::int % 2)$$,
        ARRAY[(ARRAY['OTHERS'], $$='Error'$$)]::sys_syn_dblink.exception_trap[]
);

SELECT sys_syn_dblink.processing_table_add (
        schema          => 'processor_data',
        in_table_id     => 'test_table',
        out_group_id    => 'out',
        in_group_id     => 'in',
        dblink_connname => 'sys_syn_test');


SELECT * FROM processor_data.test_table_out_claim();

SELECT * FROM processor_data.test_table_out_pull();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_out_processing
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_out_process();

SELECT  *
FROM    processor_data.test_table_out
ORDER BY test_table_id, test_table_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_out_processed
ORDER BY id;


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;