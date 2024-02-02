import pytest
import clickhouse_connect
import psycopg2 as pg


@pytest.fixture
def clickhouse_test_data():
    rows_count = 20  # How many columns generate

    client = clickhouse_connect.create_client(
        host='localhost',
        port=8123,
        username='sberpm',
        password='sberpm'
    )

    columns = r"""
    column1 Int8, column2 Int16, column3 Int32, column4 Int64, column5 UInt8, column6 UInt16, column7 UInt32, 
    column8 UInt64, column9 Int128, column10 Int256, column11 UInt128, column12 UInt256, column13 Float32, 
    column14 Float64, column15 Decimal({dec_precision}, 5), column16 String, column17 FixedString(1000), 
    column18 Enum8(\'test1\' = 1, \'test2\' = 2, \'test3\' = 3), 
    column19 Enum16(\'test1\' = 1, \'test2\' = 2, \'test3\' = 3), column20 Date, column22 DateTime, 
    column23 DateTime64(3), column24 IPv4, column25 IPv6, column26 Tuple(Int32, Int32), column28 UUID
    """

    get_rand_data_query = f"""
    SELECT * 
    FROM generateRandom(
    '{columns.format(dec_precision=15)}'
    ) 
    LIMIT {rows_count};
    """

    columns_for_table_creation = columns.replace('\\', '').format(dec_precision=20)
    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS `default`.`test_table`
        (
        {columns_for_table_creation}
        )
        ENGINE = MergeTree
        ORDER BY column1
    '''

    gen_data_query = f'''
        INSERT INTO `default`.`test_table`
        {get_rand_data_query} 
    '''

    client.query(create_table_query)
    client.query(gen_data_query)


@pytest.fixture
def postgres_test_data():
    rows_count = 20  # How many columns generate

    connection = pg.connect(
        host='localhost',
        port=5432,
        user='sberpm',
        password='sberpm',
        database='sberpm',
    )
    # columns = r'''
    # column1 cidr, column2 circle, column3 inet, column4 interval, column5 json, column6 jsonb, column7 line,
    # column8 lseg, column9 macaddr, column10 macaddr8, column20 path, column21 pg_lsn, column22 pg_snapshot,
    # column23 point, column24 polygon, column25 tsquery, column26 tsvector, column27 txid_snapshot, column28 uuid,
    # column29 xml, column30 integer, column31 char, column32 varchar, column33 date, column34 timestamp, column35 real,
    # column36 double precision, column37 decimal, column38 numeric, column39 smallint, column40 bigint, column41 bit,
    # column42 boolean, column43 bytea, column44 character, column45 character varying, column46 double precision,
    # column47 money, column48 serial, column49 smallserial, column50 text, column51 time, column52 bigserial,
    # column53 box
    # '''

    columns = r'''
    column1 bigint, column2 varchar, column3 double precision, column4 numeric, column5 date, column6 timestamp, 
    column7 bytea, column8 uuid
    '''
    get_rand_data_query = f'''
    SELECT
        generate_series(1,10) AS column1,  /* int64 */
        md5(random()::text)::text AS column2,  /* int8 */
        random() * 10 AS column3,  /* float64 */
        floor(random() * 10) AS column4,  /* int8 */
        '2000-01-01'::date + trunc(random() * 366 * 10)::int column5,  /* date */
        timestamp '2014-01-10 20:00:00' + random() * (timestamp '2014-01-20 20:00:00' - timestamp '2014-01-10 10:00:00')
            column6,  /* timestamp */
        (
            SELECT decode(string_agg(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0') ,''), 'hex')
            FROM generate_series(1, 1000)
            ) column7,  /* bytea */
        gen_random_uuid() column8  /* uuid */
    '''

    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS public.test_table
        (
        {columns}
        )
    '''

    gen_data_query = f'''
        INSERT INTO public.test_table
        {get_rand_data_query} 
    '''

    with connection:
        with connection.cursor() as cur:
            cur.execute(create_table_query)
            cur.execute(gen_data_query)
