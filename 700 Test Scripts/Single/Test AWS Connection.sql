DO
$$
    DECLARE
        i         INTEGER := 0;
        l_return  INTEGER;
        e_Context TEXT;
        e_Msg     TEXT;
        e_State   TEXT;
        l_Error   RECORD;
    BEGIN
        IF DBLINK_GET_CONNECTIONS() IS NULL
        THEN
            PERFORM DBLINK_CONNECT('aws',
                                   'host=awsfree.cbi3wlrbldxy.us-west-2.rds.amazonaws.com port=5432 dbname=sn000000 connect_timeout=9999 user=enternetadmin password=Scanner-Seduce8-Kabob-Contact-Process options=-csearch_path=');
        END IF;
        PERFORM DBLINK_EXEC('aws',
                            'UPDATE s0000v0000.exsystem SET productionversion=0 WHERE id = 1');
        LOOP
            PERFORM DBLINK_EXEC('aws',
                                'UPDATE s0000v0000.exsystem SET productionversion=productionversion + 1 WHERE id = 1');
            SELECT t1.ProductionVersion
            INTO l_return
            FROM DBLINK('aws',
                        'SELECT a.productionversion
                        FROM s0000v0000.exSystem a WHERE id = 1')
                AS t1(ProductionVersion INT);
            RAISE NOTICE 'l_return % i %', l_return, i;
            i := i + 1;
            IF i > 1000
            THEN
                EXIT;
            END IF;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                e_state = RETURNED_SQLSTATE,
                e_msg = MESSAGE_TEXT,
                e_context = PG_EXCEPTION_CONTEXT;
            l_error := fnsysError(e_state, e_msg, e_context);
            RAISE NOTICE 'l_return % i %', l_return, i;
            IF l_error.IsExceptionRaised = TRUE
            THEN
                RAISE EXCEPTION '%', l_Error.Message;
            ELSE
                RAISE NOTICE '%', l_Error.Message ;
            END IF;

    END
$$ LANGUAGE plpgsql;
/*
SELECT dblink_connect('aws','host=awsfree.cbi3wlrbldxy.us-west-2.rds.amazonaws.com port=5432 dbname=sn000000 connect_timeout=9999 user=enternetadmin password=Scanner-Seduce8-Kabob-Contact-Process options=-csearch_path=');

SELECT dblink_get_connections();

SELECT dblink_disconnect('aws');
*/