BEGIN;

CREATE EXTENSION sys_syn_dblink;
CREATE EXTENSION temporal_tables;
CREATE EXTENSION btree_gist;

INSERT INTO sys_syn_dblink.put_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 proc_table_id_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES (NULL,                   25,             '{date_infinity}',      'date',                         'test_table_bitemporal',
        null,                   null,                   null,
     $$CASE WHEN %1 <= '0001-01-01'::DATE THEN '-infinity'::DATE WHEN %1 >= '9999-12-31'::DATE THEN 'infinity'::DATE ELSE %1 END$$);


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
        in_table_id     => 'test_table_bitemporal',
        out_group_id    => 'out',
        put_group_id    => 'put',
        put_schema      => 'put_data',
        table_type_id   => 'sys_syn-bitemporal',
        dblink_connname => 'sys_syn_test');

SELECT * FROM processor_data.test_table_bitemporal_claim_1();

SELECT * FROM processor_data.test_table_bitemporal_pull_1();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_bitemporal_processing_1
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_bitemporal_process_1();

SELECT  test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_text
FROM    put_data.test_table_bitemporal
ORDER BY test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_text;

SELECT  test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_text
FROM    put_data.test_table_bitemporal_history
ORDER BY test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_text;

SELECT  test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_text
FROM ONLY put_data.test_table_bitemporal_history
ORDER BY test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_text;

SELECT  test_table_bitemporal_id
FROM    put_data.test_table_bitemporal_immutable
ORDER BY test_table_bitemporal_id;

SELECT  test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_text
FROM    put_data.test_table_bitemporal_active
ORDER BY test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_text;

SELECT  test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_text
FROM    put_data.test_table_bitemporal_current
ORDER BY test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_text;


SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_bitemporal_processed_1
ORDER BY id;


SELECT * FROM processor_data.test_table_bitemporal_push_status_1();


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
