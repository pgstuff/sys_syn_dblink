CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES  (1,              'test_data 1'),
        (2,              'test_data 2'),
        (3,              'test_data 3');

INSERT INTO sys_syn.out_groups_def VALUES ('out');
INSERT INTO sys_syn.out_groups_def VALUES ('out2');

SELECT sys_syn.out_table_create('user_data', 'test_table', 'out', data_view => TRUE);
SELECT sys_syn.out_table_create('user_data', 'test_table', 'out2');

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move_1();

SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue_1;

UPDATE  sys_syn.settings
SET     cluster_id = 'sys_syn_dblink-test';



CREATE EXTENSION hstore;

CREATE EXTENSION dblink;



CREATE TABLE public.test_data (
        test_data_id integer NOT NULL,
        test_data_text text,
        CONSTRAINT test_data_pid PRIMARY KEY (test_data_id));

INSERT INTO sys_syn.in_groups_def VALUES ('group');

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('public.test_data'::regclass, 'group');
END$$;

INSERT INTO public.test_data(
        test_data_id, test_data_text)
VALUES  (1,              'test_data 1'),
        (2,              'test_data 2'),
        (3,              'test_data 3');

INSERT INTO sys_syn.out_groups_def VALUES ('group');

SELECT sys_syn.out_table_create('public', 'test_data', 'group', data_view => TRUE);

SELECT public.test_data_pull(FALSE);
SELECT public.test_data_group_move_1();

SELECT id, delta_type, queue_state FROM public.test_data_group_queue_1;



CREATE SCHEMA "User Data"
    AUTHORIZATION postgres;

CREATE TABLE "User Data"."Test Table" (
        "Test Table Id" integer NOT NULL,
        "Test Table Text" text,
        CONSTRAINT "Test Table_pid" PRIMARY KEY ("Test Table Id"));

INSERT INTO sys_syn.in_groups_def VALUES ('In Group');

SELECT sys_syn.in_table_create_sql('"User Data"."Test Table"'::regclass, 'In Group');

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('"User Data"."Test Table"'::regclass, 'In Group');
END$$;

INSERT INTO "User Data"."Test Table"(
        "Test Table Id", "Test Table Text")
VALUES (1,              'test_data v1');

INSERT INTO sys_syn.out_groups_def VALUES ('Out Group');

DO $$BEGIN
        EXECUTE sys_syn.out_table_create_sql('"User Data"'::regnamespace, 'Test Table', 'Out Group', data_view => TRUE);
END$$;

SELECT "User Data"."Test Table_pull"(FALSE);
SELECT "User Data"."Test Table_Out Group_move_1"();



CREATE TABLE user_data.test_table_array (
        test_table_array_id integer NOT NULL,
        test_table_array_updated timestamp with time zone,
        test_table_array_text text,
        CONSTRAINT test_table_array_pid PRIMARY KEY (test_table_array_id, test_table_array_updated));

SELECT sys_syn.in_table_create (
                'user_data',
                'test_table_array',
                'in',
                NULL,
                ARRAY[
                       $COL$("test_table_array_id","integer",Id,"in_source.test_table_array_id",,,,,)$COL$,
                       $COL$("test_table_array_updated","timestamp with time zone",Attribute,"in_source.test_table_array_updated",1,,,,)$COL$,
                       $COL$("test_table_array_text","text",Attribute,"in_source.test_table_array_text",,,,,)$COL$
                ]::sys_syn.create_in_column[],
                'user_data.test_table_array',
                NULL
        );

INSERT INTO user_data.test_table_array(
        test_table_array_id, test_table_array_updated,             test_table_array_text)
VALUES  (1,              '2009-01-02 03:04:05-00',       'test_data1 v1'),
        (1,              '2010-01-02 03:04:05-00',       'test_data1 v2'),
        (2,              '2011-01-02 03:04:05-00',       'test_data2 v1'),
        (2,              '2012-01-02 03:04:05-00',       'test_data2 v2');

SELECT sys_syn.out_table_create('user_data', 'test_table_array', 'out', data_view => TRUE);

SELECT user_data.test_table_array_pull(FALSE);
SELECT user_data.test_table_array_out_move_1();

SELECT * FROM user_data.test_table_array_out_queue_data_1;



CREATE TABLE user_data.test_table_bitemporal (
        test_table_bitemporal_id integer NOT NULL,
        test_table_bitemporal_updated timestamp with time zone,
        test_table_bitemporal_start date,
        test_table_bitemporal_end date,
        test_table_bitemporal_text text,
        CONSTRAINT test_table_bitemporal_pid PRIMARY KEY (test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start));

SELECT sys_syn.in_table_create (
                'user_data',
                'test_table_bitemporal',
                'in',
                NULL,
                ARRAY[
                       $COL$("test_table_bitemporal_id","integer",Id,"in_source.test_table_bitemporal_id",,,,,)$COL$,
                       $COL$("test_table_bitemporal_updated","timestamp with time zone",Attribute,"in_source.test_table_bitemporal_updated",1,,,,)$COL$,
                       $COL$("test_table_bitemporal_start","date",Attribute,"in_source.test_table_bitemporal_start",2,,,,)$COL$,
                       $COL$("test_table_bitemporal_end","date",Attribute,"in_source.test_table_bitemporal_end",3,,,,)$COL$,
                       $COL$("test_table_bitemporal_text","text",Attribute,"in_source.test_table_bitemporal_text",,,,,)$COL$
                ]::sys_syn.create_in_column[],
                'user_data.test_table_bitemporal',
                NULL
        );

INSERT INTO user_data.test_table_bitemporal(
        test_table_bitemporal_id, test_table_bitemporal_updated, test_table_bitemporal_start, test_table_bitemporal_end, test_table_bitemporal_text)
VALUES  (1,              '2009-01-02 03:04:05-00',       '2009-01-03',  '9999-12-31',   'test_data1 no changes'),
        (2,              '2011-01-02 03:04:05-00',       '2009-01-01',  '9999-12-31',   'test_data2 v1 original'),
        (2,              '2012-01-02 03:04:05-00',       '2009-02-01',  '9999-12-31',   'test_data2 v2 forward period_2 change'),
        (3,              '2011-01-02 03:04:05-00',       '2009-01-01',  '9999-12-31',   'test_data3 v1 original'),
        (3,              '2012-01-02 03:04:05-00',       '2009-01-01',  '9999-12-31',   'test_data3 v2 replace period_2 change'),
        (4,              '2011-01-02 03:04:05-00',       '2009-01-01',  '9999-12-31',   'test_data4 v1 original'),
        (4,              '2012-01-02 03:04:05-00',       '2008-12-01',  '9999-12-31',   'test_data4 v2 backdate period_2 change'),
        (5,              '2011-01-02 03:04:05-00',       '2009-01-01',  '9999-12-31',   'test_data5 v1 original'),
        (5,              '2012-01-02 03:04:05-00',       '2009-01-01',  '9999-12-31',   'test_data5 v2 replace period_2 change'),
        (5,              '2013-01-02 03:04:05-00',       '2009-03-01',  '9999-12-31',   'test_data5 v3 forward period_2 change'),
        (5,              '2014-01-02 03:04:05-00',       '2009-02-01',  '9999-12-31',   'test_data5 v4 backdate period_2 change');

SELECT sys_syn.out_table_create('user_data', 'test_table_bitemporal', 'out', data_view => TRUE);

SELECT user_data.test_table_bitemporal_pull(FALSE);
SELECT user_data.test_table_bitemporal_out_move_1();

SELECT * FROM user_data.test_table_bitemporal_out_queue_data_1;



CREATE TABLE user_data.composite_key_table (
        key_1   integer         NOT NULL,
        key_2   bigint          NOT NULL,
        key_3   smallint        NOT NULL,
        key_4   smallint        NOT NULL,
        key_5   smallint        NOT NULL,
        key_6   smallint        NOT NULL,
        data_1  text,
        data_2  text,
        data_3  text,
        data_4  text,
        data_5  text,
        data_6  text,
        CONSTRAINT composite_key_table_pid PRIMARY KEY (key_1, key_2, key_3, key_4, key_5, key_6));

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.composite_key_table'::regclass, 'in');
END$$;

INSERT INTO user_data.composite_key_table(
        key_1,  key_2,  key_3,  key_4,  key_5,  key_6,  data_1, data_2, data_3, data_4, data_5, data_6)
VALUES (1,      1,      1,      1,      1,      6,      '1-1',  '1-2',  '1-3',  '1-4',  '1-5',  '1-6'), (
        1,      1,      2,      1,      2,      5,      '2-1',  '2-2',  '2-3',  '2-4',  '2-5',  '2-6'), (
        1,      1,      2,      2,      3,      4,      '3-1',  '3-2',  '3-3',  '3-4',  '3-5',  '3-6'), (
        1,      2,      3,      1,      4,      3,      '4-1',  '4-2',  '4-3',  '4-4',  '4-5',  '4-6'), (
        1,      2,      3,      2,      5,      2,      '5-1',  '5-2',  '5-3',  '5-4',  '5-5',  '5-6'), (
        1,      2,      3,      3,      6,      1,      '6-1',  '6-2',  '6-3',  '6-4',  '6-5',  '6-6');

SELECT sys_syn.out_table_create('user_data', 'composite_key_table', 'out', data_view => TRUE);

SELECT user_data.composite_key_table_pull(FALSE);
SELECT user_data.composite_key_table_out_move_1();

SELECT id, delta_type, queue_state FROM user_data.composite_key_table_out_queue_1;



SELECT sys_syn.in_table_create (
                'user_data',
                'composite_key_array',
                'in',
                NULL,
                ARRAY[
                       $COL$("key_1","integer",Id,"in_source.key_1",,,,,)$COL$,
                       $COL$("key_2","integer",Id,"in_source.key_2",,,,,)$COL$,
                       $COL$("key_3","integer",Id,"in_source.key_3",,,,,)$COL$,
                       $COL$("key_4","text",Attribute,"in_source.key_4",1,,,,)$COL$,
                       $COL$("key_5","text",Attribute,"in_source.key_5",2,,,,)$COL$,
                       $COL$("key_6","text",Attribute,"in_source.key_6",3,,,,)$COL$,
                       $COL$("data_1","text",Attribute,"in_source.data_1",,,,,)$COL$,
                       $COL$("data_2","text",Attribute,"in_source.data_2",,,,,)$COL$,
                       $COL$("data_3","text",Attribute,"in_source.data_3",,,,,)$COL$,
                       $COL$("data_4","text",Attribute,"in_source.data_4",,,,,)$COL$,
                       $COL$("data_5","text",Attribute,"in_source.data_5",,,,,)$COL$,
                       $COL$("data_6","text",Attribute,"in_source.data_6",,,,,)$COL$
                ]::sys_syn.create_in_column[],
                'user_data.composite_key_table',
                NULL
        );

SELECT sys_syn.out_table_create('user_data', 'composite_key_array', 'out', data_view => TRUE);

SELECT user_data.composite_key_array_pull(FALSE);
SELECT user_data.composite_key_array_out_move_1();

SELECT * FROM user_data.composite_key_array_out_queue_data_1;



CREATE TABLE user_data.parent_table (
        parent_table_id integer NOT NULL,
        parent_table_text text,
        CONSTRAINT parent_table_pid PRIMARY KEY (parent_table_id));

CREATE TABLE user_data.child_table (
        child_table_id integer NOT NULL,
        parent_table_id integer,
        CONSTRAINT child_table_pid PRIMARY KEY (child_table_id));

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.parent_table'::regclass, 'in');
END$$;

SELECT sys_syn.in_table_create (
                schema          => 'user_data',
                in_table_id     => 'child_table',
                in_group_id     => 'in',
                in_pull_id      => NULL,
                in_columns      => ARRAY[
                       $COL$("child_table_id","integer",Id,"in_source.child_table_id",,,,,)$COL$,
                       $COL$("parent_table_id","integer",Attribute,"in_source.parent_table_id",,1,"parent_table","parent_table_id",)$COL$
                ]::sys_syn.create_in_column[],
                full_table_reference    => 'user_data.child_table'
        );

INSERT INTO user_data.parent_table(
        parent_table_id,       parent_table_text)
VALUES (1,                      'parent_data');

INSERT INTO user_data.child_table(
        child_table_id,        parent_table_id)
VALUES (2,                      1);

SELECT sys_syn.out_table_create('user_data', 'parent_table', 'out', data_view => TRUE);

SELECT sys_syn.out_table_create('user_data', 'child_table', 'out', data_view => TRUE);

ALTER TABLE user_data.parent_table_out_queue_1
  ADD FOREIGN KEY (trans_id_in, id) REFERENCES user_data.parent_table_in_1 (trans_id_in, id) ON UPDATE RESTRICT ON DELETE RESTRICT;

SELECT user_data.parent_table_pull(FALSE);
SELECT user_data.parent_table_out_move_1();



CREATE TABLE user_data.test_table_part (
        test_table_id integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_part_pid PRIMARY KEY (test_table_id));

DO $$BEGIN
        EXECUTE sys_syn.in_table_create_sql('user_data.test_table_part'::regclass, 'in', in_partition_count => 3::smallint);
END$$;

SELECT sys_syn.out_table_create('user_data', 'test_table_part', 'out', data_view => TRUE);

INSERT INTO user_data.test_table_part(
        test_table_id,          test_table_text)
SELECT  generate_series,        'test_data ' || generate_series
FROM    generate_series(1, 10);

SELECT user_data.test_table_part_pull(FALSE);
SELECT user_data.test_table_part_out_move_1();
SELECT user_data.test_table_part_out_move_2();
SELECT user_data.test_table_part_out_move_3();

SELECT id, delta_type, queue_state FROM user_data.test_table_part_out_queue_1;
SELECT id, delta_type, queue_state FROM user_data.test_table_part_out_queue_2;
SELECT id, delta_type, queue_state FROM user_data.test_table_part_out_queue_3;



CREATE TABLE user_data.test_table_range (
        test_table_range_id integer NOT NULL,
        test_table_range_time_start timestamp with time zone NOT NULL,
        test_table_range_time_end timestamp with time zone,
        test_table_range_date_start date NOT NULL,
        test_table_range_date_end date,
        test_table_range_int_start smallint NOT NULL,
        test_table_range_int_end smallint,
        test_table_range_text text,
        CONSTRAINT test_table_range_pid PRIMARY KEY (test_table_range_id, test_table_range_time_start, test_table_range_date_start, test_table_range_int_start));

SELECT sys_syn.in_table_create (
                'user_data',
                'test_table_range',
                'in',
                NULL,
                ARRAY[
                       $COL$("test_table_range_id","integer",Id,"in_source.test_table_range_id",,,,,)$COL$,
                       $COL$("test_table_range_time_start","timestamp with time zone",Attribute,"in_source.test_table_range_time_start",1,,,,)$COL$,
                       $COL$("test_table_range_time_end","timestamp with time zone",Attribute,"in_source.test_table_range_time_end",2,,,,)$COL$,
                       $COL$("test_table_range_date_start","date",Attribute,"in_source.test_table_range_date_start",3,,,,)$COL$,
                       $COL$("test_table_range_date_end","date",Attribute,"in_source.test_table_range_date_end",4,,,,)$COL$,
                       $COL$("test_table_range_int_start","smallint",Attribute,"in_source.test_table_range_int_start",5,,,,)$COL$,
                       $COL$("test_table_range_int_end","smallint",Attribute,"in_source.test_table_range_int_end",6,,,,)$COL$,
                       $COL$("test_table_range_text","text",Attribute,"in_source.test_table_range_text",,,,,)$COL$
                ]::sys_syn.create_in_column[],
                'user_data.test_table_range',
                NULL
        );

INSERT INTO user_data.test_table_range(
        test_table_range_id, test_table_range_time_start, test_table_range_time_end, test_table_range_date_start, test_table_range_date_end, test_table_range_int_start, test_table_range_int_end, test_table_range_text)
VALUES  (1,     '2001-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-01'::DATE, null,                   1, 2,   'Test 1'),
        (1,     '2001-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-02'::DATE, null,                   1, 2,   'Test 2'),
        (1,     '2001-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     1, 2,   'Test 3'),
        (1,     '2001-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     2, null,'Test 4'),
        (1,     '2001-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     3, 4,   'Test 5'),
        (1,     '2002-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-01'::DATE, null,                   1, 2,   'Test 6'),
        (1,     '2002-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-02'::DATE, null,                   1, null,'Test 7'),
        (1,     '2002-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     1, null,'Test 8'),
        (1,     '2002-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     2, null,'Test 9'),
        (1,     '2002-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     3, null,'Test 10'),
        (1,     '2003-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-01'::DATE, null,                   1, null,'Test 11'),
        (1,     '2003-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-02'::DATE, null,                   1, 2,   'Test 12'),
        (1,     '2003-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     1, 2,   'Test 13');



SELECT sys_syn.out_table_create('user_data', 'test_table_range', 'out', data_view => TRUE);

SELECT user_data.test_table_range_pull(FALSE);
SELECT user_data.test_table_range_out_move_1();

SELECT * FROM user_data.test_table_range_out_queue_data_1;
