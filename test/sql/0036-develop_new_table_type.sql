BEGIN;

CREATE EXTENSION sys_syn_dblink;
CREATE EXTENSION btree_gist;


CREATE SCHEMA processor_data
        AUTHORIZATION postgres;

CREATE SCHEMA put_data
        AUTHORIZATION postgres;

CREATE TYPE processor_data.test_table_range_proc_attributes AS (
        test_table_range_time_start timestamp with time zone,
        test_table_range_time_end timestamp with time zone,
        test_table_range_date_start date,
        test_table_range_date_end date,
        test_table_range_int_start smallint,
        test_table_range_int_end smallint,
        test_table_range_text text);
CREATE TYPE processor_data.test_table_range_proc_id AS (
        test_table_range_id integer);
CREATE TYPE processor_data.test_table_range_proc_no_diff AS (
        );

CREATE TABLE put_data.test_table_range (
        test_table_range_id integer NOT NULL,
        test_table_range_time_start tstzrange NOT NULL,
        test_table_range_date_start daterange NOT NULL,
        test_table_range_int_start int4range NOT NULL,
        test_table_range_text text,
        CONSTRAINT test_table_range_overlap EXCLUDE
        USING gist (test_table_range_id WITH =, test_table_range_time_start WITH &&, test_table_range_date_start WITH &&, test_table_range_int_start WITH &&)
);

CREATE OR REPLACE FUNCTION processor_data.test_table_range_put(
    trans_id_in integer,
    delta_type sys_syn_dblink.delta_type,
    queue_priority smallint,
    hold_updated boolean,
    prior_hold_reason_count integer,
    prior_hold_reason_id integer,
    prior_hold_reason_text text,
    id processor_data.test_table_range_proc_id,
    attributes processor_data.test_table_range_proc_attributes[],
    no_diff processor_data.test_table_range_proc_no_diff)
  RETURNS sys_syn_dblink.processed_status AS
$BODY$
DECLARE
        _processed_status               sys_syn_dblink.processed_status;
        _exception_sql_state            text;
        _exception_message              text;
        _exception_detail               text;
        _exception_hint                 text;
        _exception_context              text;
        attribute_rows                  processor_data.test_table_range_proc_attributes;
        _range_1_lower                  timestamp with time zone;
        _range_1_lower_next             timestamp with time zone := null;
        _range_1_lower_next_change      timestamp with time zone := null;
        _range_2_lower                  date;
        _range_2_lower_next             date := null;
        _range_2_lower_next_change      date := null;
        _range_3_lower                  smallint;
        _range_3_lower_next             smallint := null;
        _range_3_lower_next_change      smallint := null;
BEGIN
        DELETE FROM put_data.test_table_range AS out_table
        WHERE   out_table.test_table_range_id = id.test_table_range_id;

        IF delta_type != 'Delete'::sys_syn_dblink.delta_type THEN
                FOR _array_index IN REVERSE array_length(attributes, 1) .. 1 LOOP
                        attribute_rows  := attributes[_array_index];
                        _range_1_lower  := attribute_rows.test_table_range_time_start;
                        _range_2_lower  := attribute_rows.test_table_range_date_start;
                        _range_3_lower  := attribute_rows.test_table_range_int_start;

                        IF _range_1_lower != _range_1_lower_next THEN
                                _range_1_lower_next_change := _range_1_lower_next;
                                _range_2_lower_next_change := null;
                                _range_3_lower_next_change := null;
                        ELSIF _range_2_lower != _range_2_lower_next THEN
                                _range_2_lower_next_change := _range_2_lower_next;
                                _range_3_lower_next_change := null;
                        ELSIF _range_3_lower != _range_3_lower_next THEN
                                _range_3_lower_next_change := _range_3_lower_next;
                        END IF;

                        INSERT INTO put_data.test_table_range AS out_table (
                                test_table_range_id,
                                test_table_range_time_start,
                                test_table_range_date_start,
                                test_table_range_int_start,
                                test_table_range_text)
                        VALUES (id.test_table_range_id,
                                tstzrange(_range_1_lower, COALESCE(attribute_rows.test_table_range_time_end, _range_1_lower_next_change)),
                                daterange(_range_2_lower, COALESCE(attribute_rows.test_table_range_date_end, _range_2_lower_next_change)),
                                int4range(_range_3_lower, COALESCE(attribute_rows.test_table_range_int_end, _range_3_lower_next_change)),
                                attribute_rows.test_table_range_text);

                        _range_1_lower_next := _range_1_lower;
                        _range_2_lower_next := _range_2_lower;
                        _range_3_lower_next := _range_3_lower;
                END LOOP;
        END IF;

        RETURN _processed_status;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 5000;

SELECT processor_data.test_table_range_put(
        1,
        'Add'::sys_syn_dblink.delta_type,
        1::SMALLINT,
        false,
        1,
        2,
        ''::text,
        ROW(1)::processor_data.test_table_range_proc_id,
        ARRAY[
                ROW('2001-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-01'::DATE, null,                   1, 2,   'Test 1'),
                ROW('2001-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-02'::DATE, null,                   1, 2,   'Test 2'),
                ROW('2001-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     1, 2,   'Test 3'),
                ROW('2001-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     2, null,'Test 4'),
                ROW('2001-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     3, 4,   'Test 5'),
                ROW('2002-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-01'::DATE, null,                   1, 2,   'Test 6'),
                ROW('2002-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-02'::DATE, null,                   1, null,'Test 7'),
                ROW('2002-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     1, null,'Test 8'),
                ROW('2002-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     2, null,'Test 9'),
                ROW('2002-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     3, null,'Test 10'),
                ROW('2003-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-01'::DATE, null,                   1, null,'Test 11'),
                ROW('2003-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-02'::DATE, null,                   1, 2,   'Test 12'),
                ROW('2003-01-01 12:34:51.00000-00'::timestamp with time zone, null, '2000-01-03'::DATE, '2000-01-04'::DATE,     1, 2,   'Test 13')
        ]::processor_data.test_table_range_proc_attributes[],
        ROW()::processor_data.test_table_range_proc_no_diff);

SELECT  test_table_range_id, test_table_range_time_start, test_table_range_date_start, test_table_range_int_start, test_table_range_text
FROM    put_data.test_table_range
ORDER BY test_table_range_id, test_table_range_time_start, test_table_range_date_start, test_table_range_int_start;

CREATE OR REPLACE FUNCTION sys_syn_dblink.put_sql_range_array (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        schema_processed_name           text,
        type_id_name                    text,
        type_attributes_name            text,
        type_no_diff_name               text,
        table_settings                  hstore)
        RETURNS sys_syn_dblink.put_code_sql AS
$BODY$
DECLARE
        _table_name_sql         TEXT;
        _columns_id             sys_syn_dblink.create_put_column[];
        _columns_orderby        sys_syn_dblink.create_put_column[];
        _columns_unordered      sys_syn_dblink.create_put_column[];
        _put_code_sql           sys_syn_dblink.put_code_sql;
        _range_count            smallint;
        _range_index            smallint;
        _put_column_index       smallint;
        _range_index_inner      smallint;
        _range_put_column_lower sys_syn_dblink.create_put_column;
        _range_put_column_upper sys_syn_dblink.create_put_column;
        _range_type             TEXT;
        _range_id               TEXT;
        _range_set_lowers_sql   TEXT := '';
        _range_set_change_sql   TEXT := '';
        _range_columns_sql      TEXT := '';
        _range_values_sql       TEXT := '';
        _range_set_next_sql     TEXT := '';
BEGIN
        _table_name_sql         := schema_name::text || '.' || quote_ident(table_name);
        _columns_id             := sys_syn_dblink.put_columns_query_unique_index(put_columns);
        _columns_orderby        := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, TRUE);
        _columns_unordered      := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, FALSE);

        _range_count := (
                        SELECT  MAX(array_order)
                        FROM    unnest(_columns_orderby)
                );
        IF (_range_count % 2) != 0 THEN
                RAISE EXCEPTION 'sys_syn_dblink.put_sql_range_array:  The max array_order must be an even number.';
        END IF;
        _range_count := _range_count / 2;

        _put_code_sql.declarations_sql := $$
        attribute_rows                  $$||quote_ident(schema_processed_name)||'.'||quote_ident(type_attributes_name)||$$;$$;

        FOR _range_index IN 0 .. _range_count - 1 LOOP
                _range_put_column_lower := NULL;
                _range_put_column_upper := NULL;
                FOR _put_column_index IN 1 .. array_length(_columns_orderby, 1) LOOP
                        IF _columns_orderby[_put_column_index].array_order = _range_index * 2 + 1 THEN
                                _range_put_column_lower = _columns_orderby[_put_column_index];
                        ELSIF _columns_orderby[_put_column_index].array_order = _range_index * 2 + 2 THEN
                                _range_put_column_upper = _columns_orderby[_put_column_index];
                        END IF;
                END LOOP;
                IF _range_put_column_lower IS NULL THEN
                        RAISE EXCEPTION 'sys_syn_dblink.put_sql_range_array:  Cannot find lower range %, array_order %', _range_index + 1, _range_index * 2 + 1;
                ELSIF _range_put_column_upper IS NULL THEN
                        RAISE EXCEPTION 'sys_syn_dblink.put_sql_range_array:  Cannot find upper range %, array_order %', _range_index + 1, _range_index * 2 + 2;
                ELSIF _range_put_column_lower.data_type != _range_put_column_upper.data_type THEN
                        RAISE EXCEPTION 'sys_syn_dblink.put_sql_range_array:  In range %, array_order % data type ''%'' does not match array_order % data type ''%''.', _range_index + 1, _range_index * 2 + 1, _range_put_column_lower.data_type, _range_index * 2 + 2, _range_put_column_upper.data_type;
                END IF;

                _range_type := sys_syn_dblink.range_from_data_type(_range_put_column_lower.data_type);
                _range_id := '_range_' || (_range_index+1)::TEXT || '_';

                _put_code_sql.declarations_sql := _put_code_sql.declarations_sql || $$
        $$ || _range_id || $$lower                  $$ || _range_put_column_lower.data_type || $$;
        $$ || _range_id || $$lower_next             $$ || _range_put_column_lower.data_type || $$ := null;
        $$ || _range_id || $$lower_next_change      $$ || _range_put_column_lower.data_type || $$ := null;$$;

                _range_set_lowers_sql := _range_set_lowers_sql || $$
                        $$ || _range_id || $$lower  := $$ || _range_put_column_lower.value_expression || $$;$$;

                IF _range_index = 0 THEN
                        _range_set_change_sql := _range_set_change_sql || $$
                        IF _range_1_lower != _range_1_lower_next THEN$$;
                ELSE
                        _range_set_change_sql := _range_set_change_sql || $$
                        ELSIF $$ || _range_id || $$lower != $$ || _range_id || $$lower_next THEN$$;
                END IF;
                _range_set_change_sql := _range_set_change_sql || $$
                                $$ || _range_id || $$lower_next_change := $$ || _range_id || $$lower_next;$$;
                FOR _range_index_inner IN _range_index + 1 .. _range_count - 1 LOOP
                        _range_set_change_sql := _range_set_change_sql || $$
                                _range_$$ || (_range_index_inner+1)::TEXT || $$_lower_next_change := null;$$;
                END LOOP;

                _range_columns_sql := _range_columns_sql || $$,
                                $$ || _range_put_column_lower.column_name;

                _range_values_sql := _range_values_sql || $$,
                                $$ || _range_type || $$($$ || _range_id || $$lower, COALESCE($$ || _range_put_column_upper.value_expression || $$, $$ || _range_id || $$lower_next_change))$$;

                _range_set_next_sql := _range_set_next_sql || $$
                        $$ || _range_id || $$lower_next := $$ || _range_id || $$lower;$$;
        END LOOP;

        _put_code_sql.logic_sql := $$
        DELETE FROM $$||_table_name_sql||$$ AS out_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,
        'out_table.%COLUMN_NAME% = %VALUE_EXPRESSION%', ' AND
                ')||$$;

        IF delta_type != 'Delete'::sys_syn_dblink.delta_type THEN
                FOR _array_index IN REVERSE array_length(attributes, 1) .. 1 LOOP
                        attribute_rows  := attributes[_array_index];$$||_range_set_lowers_sql||$$
$$||_range_set_change_sql||$$
                        END IF;

                        INSERT INTO $$||_table_name_sql||$$ AS out_table (
                                $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ',
                                ')||_range_columns_sql||sys_syn_dblink.put_columns_format(_columns_unordered, ',
                                %COLUMN_NAME%', '')||$$)
                        VALUES ($$||sys_syn_dblink.put_columns_format(_columns_id, '%VALUE_EXPRESSION%', ',
                                ')||_range_values_sql||sys_syn_dblink.put_columns_format(_columns_unordered, ',
                                %VALUE_EXPRESSION%', '')||$$);
$$||_range_set_next_sql||$$
                END LOOP;
        END IF;$$;

        RETURN _put_code_sql;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

SELECT * FROM sys_syn_dblink.put_sql_range_array(
        schema_name                     => 'put_data',
        table_name                      => 'test_table_range',
        put_columns                     => ARRAY[
                ROW(    'test_table_range_id',          'integer',                      'Id',
                        $$id.test_table_range_id$$,
                        NULL,   'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_time_start',  'timestamp with time zone',     'Attribute',
                        $$attribute_rows.test_table_range_time_start$$,
                        1,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_time_end',    'timestamp with time zone',     'Attribute',
                        $$attribute_rows.test_table_range_time_end$$,
                        2,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_date_start',  'date',                         'Attribute',
                        $$attribute_rows.test_table_range_date_start$$,
                        3,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_date_end',    'date',                         'Attribute',
                        $$attribute_rows.test_table_range_date_end$$,
                        4,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_int_start',   'smallint',                     'Attribute',
                        $$attribute_rows.test_table_range_int_start$$,
                        5,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_int_end',     'smallint',                     'Attribute',
                        $$attribute_rows.test_table_range_int_end$$,
                        6,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_text',        'text',                         'Attribute',
                        $$attribute_rows.test_table_range_text$$,
                        NULL,   'Here', FALSE,  ARRAY[]::TEXT[],        NULL)
        ]::sys_syn_dblink.create_put_column[],
        schema_processed_name           => 'processor_data',
        type_id_name                    => 'test_table_range_proc_id',
        type_attributes_name            => 'test_table_range_proc_attributes',
        type_no_diff_name               => 'test_table_range_proc_no_diff',
        table_settings                  => ''
        );

CREATE OR REPLACE FUNCTION sys_syn_dblink.table_create_sql_range_array (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        table_settings                  hstore)
        RETURNS text AS
$BODY$
DECLARE
        _table_name_sql         TEXT;
        _columns_id             sys_syn_dblink.create_put_column[];
        _columns_orderby        sys_syn_dblink.create_put_column[];
        _columns_unordered      sys_syn_dblink.create_put_column[];
        _range_count            smallint;
        _range_index            smallint;
        _range_put_column_lower sys_syn_dblink.create_put_column;
        _range_type             TEXT;
        _columns_orderby_sql    TEXT := '';
        _columns_constraint_sql TEXT := '';
        _sql_buffer             TEXT;
BEGIN
        _table_name_sql         := schema_name::text || '.' || quote_ident(table_name);
        _columns_id             := sys_syn_dblink.put_columns_query_unique_index(put_columns);
        _columns_orderby        := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, TRUE);
        _columns_unordered      := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, FALSE);

        _range_count := (
                        SELECT  MAX(array_order)
                        FROM    unnest(_columns_orderby)
                );
        IF (_range_count % 2) != 0 THEN
                RAISE EXCEPTION 'sys_syn_dblink.put_sql_range_array:  The max array_order must be an even number.';
        END IF;
        _range_count := _range_count / 2;

        FOR _range_index IN 0 .. _range_count - 1 LOOP
                _range_put_column_lower := NULL;
                FOR _put_column_index IN 1 .. array_length(_columns_orderby, 1) LOOP
                        IF _columns_orderby[_put_column_index].array_order = _range_index * 2 + 1 THEN
                                _range_put_column_lower = _columns_orderby[_put_column_index];
                        END IF;
                END LOOP;
                IF _range_put_column_lower IS NULL THEN
                        RAISE EXCEPTION 'sys_syn_dblink.put_sql_range_array:  Cannot find lower range %, array_order %', _range_index + 1, _range_index * 2 + 1;
                END IF;

                _range_type := sys_syn_dblink.range_from_data_type(_range_put_column_lower.data_type);

                _columns_orderby_sql := _columns_orderby_sql || $$,
        $$ || _range_put_column_lower.column_name || $$ $$ || _range_type || $$ NOT NULL$$;

                _columns_constraint_sql := _columns_constraint_sql || $$, $$ || _range_put_column_lower.column_name || $$ WITH &&$$;
        END LOOP;

        _sql_buffer := $$CREATE TABLE $$||_table_name_sql||$$ (
        $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME% %FORMAT_TYPE% NOT NULL', ',
        ')||_columns_orderby_sql||sys_syn_dblink.put_columns_format(_columns_unordered, ',
        %COLUMN_NAME% %FORMAT_TYPE%', '')||$$,
        CONSTRAINT test_table_range_overlap EXCLUDE
        USING gist ($$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME% WITH =', ', ')||_columns_constraint_sql||$$)
);
$$;

        RETURN _sql_buffer;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

SELECT * FROM sys_syn_dblink.table_create_sql_range_array(
        schema_name                     => 'put_data',
        table_name                      => 'test_table_range',
        put_columns                     => ARRAY[
                ROW(    'test_table_range_id',          'integer',                      'Id',
                        $$id.test_table_range_id$$,
                        NULL,   'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_time_start',  'timestamp with time zone',     'Attribute',
                        $$attribute_rows.test_table_range_time_start$$,
                        1,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_time_end',    'timestamp with time zone',     'Attribute',
                        $$attribute_rows.test_table_range_time_end$$,
                        2,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_date_start',  'date',                         'Attribute',
                        $$attribute_rows.test_table_range_date_start$$,
                        3,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_date_end',    'date',                         'Attribute',
                        $$attribute_rows.test_table_range_date_end$$,
                        4,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_int_start',   'smallint',                     'Attribute',
                        $$attribute_rows.test_table_range_int_start$$,
                        5,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_int_end',     'smallint',                     'Attribute',
                        $$attribute_rows.test_table_range_int_end$$,
                        6,      'Here', FALSE,  ARRAY[]::TEXT[],        NULL),
                ROW(    'test_table_range_text',        'text',                         'Attribute',
                        $$attribute_rows.test_table_range_text$$,
                        NULL,   'Here', FALSE,  ARRAY[]::TEXT[],        NULL)
        ]::sys_syn_dblink.create_put_column[],
        table_settings                  => ''
        );

ROLLBACK;
