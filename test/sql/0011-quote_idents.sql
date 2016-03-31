BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA "Reader Schema"
        AUTHORIZATION postgres;

SELECT dblink_connect('SysSyn Data', 'dbname=contrib_regression');
SELECT dblink_exec('SysSyn Data', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('In Group');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('Out Group');

SELECT sys_syn_dblink.reading_table_add (
        schema          => 'Reader Schema',
        in_table_id     => 'Test Table',
        out_group_id    => 'Out Group',
        in_group_id     => 'In Group',
        dblink_connname => 'SysSyn Data');


SELECT * FROM "Reader Schema"."Test Table_Out Group_claim"();

SELECT * FROM "Reader Schema"."Test Table_Out Group_pull"();

SELECT * FROM "Reader Schema"."Test Table_Out Group_process"();

SELECT * FROM "Reader Schema"."Test Table_Out Group_push_status"();


SELECT dblink_exec('SysSyn Data', 'ROLLBACK');
SELECT dblink_disconnect('SysSyn Data');

ROLLBACK;
