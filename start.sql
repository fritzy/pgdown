CREATE TABLE pgdown_default(key text, bucket text, value json, indexed json);

CREATE OR REPLACE FUNCTION pgdown_replace_row(tname TEXT, bucket TEXT, key TEXT, value TEXT, indexed JSON) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        EXECUTE 'UPDATE '
        || quote_ident(tname)
        || ' SET value = '
        || quote_nullable(value)
        || ', indexed = '
        || quote_nullable(indexed)
        || ' WHERE key = '
        || quote_literal(key)
        || ' AND bucket = '
        || quote_literal(bucket);
        IF found THEN
            RAISE WARNING 'UPDATE % SET value = %, indexed = % WHERE key = % AND bucket = %', quote_ident(tname), quote_nullable(value), quote_nullable(indexed), quote_literal(key), quote_literal(bucket);
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            EXECUTE 'INSERT INTO ' || quote_ident(tname) || ' (value, indexed, key, bucket) values ( ' || quote_nullable(value) || ', ' || quote_nullable(indexed) || ', ' || quote_literal(key) || ', ' || quote_literal(bucket) || ')';
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
            RETURN;
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

DROP FUNCTION pgdown_get_key_range(text,text,text,text,text,text,integer,text);
CREATE FUNCTION pgdown_get_key_range(tname TEXT, bucket TEXT, lowop TEXT, low TEXT, highop TEXT, high TEXT, count INTEGER, ascdesc TEXT) RETURNS TABLE(key text, value text) AS
$BODY$
DECLARE
    querystr TEXT;
BEGIN
    querystr := 'SELECT key::text, value::text FROM '
    || quote_ident(tname)
    || ' WHERE bucket = ' || quote_literal(bucket)
    || ' AND key ' || lowop || ' ' || quote_literal(low)
    || ' AND key ' || highop || ' ' || quote_literal(high)
    || ' ORDER BY key '
    || ascdesc;
    IF count > 0 THEN
        querystr := querystr || ' LIMIT ' || quote_literal(count);
    END IF;
    RETURN query
    EXECUTE querystr;
END;
$BODY$
LANGUAGE plpgsql;

DROP FUNCTION pgdown_get_index_range(text,text,text,text,text,text,text,integer,text);
CREATE FUNCTION pgdown_get_range_index(tname TEXT, bucket TEXT, field TEXT, lowop TEXT, low TEXT, highop TEXT, high TEXT, count INTEGER, ascdesc TEXT) RETURNS TABLE(key text, value text) AS
$BODY$
DECLARE
    querystr TEXT;
BEGIN
    querystr := 'SELECT key, value::text FROM '
    || quote_ident(tname)
    || ' WHERE bucket = ' || quote_literal(bucket)
    || ' AND indexed->' || quote_literal(field) || ' ' || lowop || ' ' || quote_literal(low)
    || ' AND indexed->'  || quote_literal(field) || ' ' || highop || ' ' || quote_literal(high)
    || ' ORDER BY indexed->'|| quote_literal(field)
    || ' ' || ascdesc;
    IF count > 0 THEN
        querystr := querystr || ' LIMIT ' || quote_literal(count);
    END IF;
    return query
    EXECUTE querystr;
END;
$BODY$
LANGUAGE plpgsql;
