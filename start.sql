DROP TABLE pgdown_keystore;
CREATE TABLE pgdown_keystore(model text, key bytea, bucket text, value bytea, indexed json);

CREATE INDEX pgdown_keystore_model_bucket_key_idx ON pgdown_keystore (model, bucket, key);

CREATE OR REPLACE FUNCTION pgdown_put(model TEXT, bucket TEXT, key TEXT, value TEXT, indexed JSON) RETURNS VOID AS
$$
 BEGIN
     LOOP
         -- first try to update the key
         EXECUTE format('UPDATE pgdown_keystore set value = %L, indexed = %L WHERE key = %L AND bucket = %L AND model = %L', value, indexed, key, bucket, model);
         IF found THEN
             RETURN;
         END IF;
         -- not there, so try to insert the key
         -- if someone else inserts the same key concurrently,
         -- we could get a unique-key failure
         BEGIN
             EXECUTE format('INSERT INTO pgdown_keystore (value, indexed, key, bucket, model) values (%L, %L, %L, %L, %L)', value, indexed, key, bucket, model);
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
CREATE FUNCTION pgdown_get_key_range(model TEXT, bucket TEXT, lowop TEXT, low TEXT, highop TEXT, high TEXT, count INTEGER, ascdesc TEXT) RETURNS TABLE(key text, value text) AS
$BODY$
DECLARE
querystr TEXT;
BEGIN
    querystr := format('SELECT key, value FROM pgdown_keystore WHERE model = %L AND bucket = %L AND key %s %L AND key %s %L ORDER BY key %s', model, bucket, lowop, low, highop, high, ascdesc); 
    IF count > 0 THEN
        querystr := querystr || format(' LIMIT %L', count);
    END IF;
    RETURN query 
    EXECUTE querystr;
END; 
$BODY$
LANGUAGE plpgsql;

DROP FUNCTION pgdown_get_range_index(text,text,text,text,text,text,text,integer,text);
CREATE FUNCTION pgdown_get_range_index(model TEXT, bucket TEXT, field TEXT, lowop TEXT, low TEXT, highop TEXT, high TEXT, count INTEGER, ascdesc TEXT) RETURNS TABLE(key text, value text) AS
$BODY$
DECLARE
querystr TEXT;
BEGIN
    querystr := format('SELECT key, value FROM pgdown_keystore WHERE model = %L AND indexed->%L %s %L AND indexed->%L %s %L ORDER BY indexed->%L %s', model, field, lowop, low, field, highop, high, field);
    IF count > 0 THEN
        querystr := querystr || format(' LIMIT %L', count);
    END IF;
    return query 
    EXECUTE querystr;
END; 
$BODY$
LANGUAGE plpgsql;
