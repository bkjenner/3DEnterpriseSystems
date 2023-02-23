
DO
$$
    DECLARE
		l_glrateid BIGINT;
    BEGIN

        delete from glrate;
        PERFORM SETVAL('glRate_id_seq', 1);

        INSERT INTO glrate (
            temporalstartdate,
            temporalenddate,
            comprovincestateid,
            glratetypeid,
            description,
            rate,
            rowstatus,
            syschangehistoryid)
        SELECT '2019-01-01'::DATE,
               '2019-12-31'::DATE,
               null::bigint,
               1,
               'Gst description',
               .06,
               'd',
               null::bigint
		RETURNING ID into l_glrateid;


        INSERT INTO glrate (
            id,
			temporalstartdate,
            temporalenddate,
            comprovincestateid,
            glratetypeid,
            description,
            rate,
            rowstatus,
            syschangehistoryid)
        SELECT l_glrateid,
				'2020-01-01'::DATE,
               '2020-12-31'::DATE,
               null::bigint,
               1,
               'Gst description',
               .06,
               'a',
               null::bigint
        union all
        SELECT l_glrateid,
				'2021-01-01'::DATE,
               '2021-12-31'::DATE,
               null::bigint,
               1,
               'Gst description',
               .05,
               'a',
               null::bigint;

        call spsysTemporalDataNormalize(p_table:='glrate',p_debug:=false);

    END
$$

/*

 select id, temporalstartdate, temporalenddate, rate, rowstatus from glrate order by id, temporalstartdate;

 */