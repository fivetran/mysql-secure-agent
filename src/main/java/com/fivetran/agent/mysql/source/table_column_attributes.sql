SELECT
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    c.COLUMN_NAME,
    c.ORDINAL_POSITION,
    c.COLUMN_TYPE,
    c.CHARACTER_SET_NAME,
    c.COLUMN_KEY,
    k.REFERENCED_TABLE_SCHEMA,
    k.REFERENCED_TABLE_NAME,
    k.REFERENCED_COLUMN_NAME
FROM information_schema.COLUMNS c
LEFT JOIN information_schema.KEY_COLUMN_USAGE k
    ON k.TABLE_SCHEMA = c.TABLE_SCHEMA COLLATE 'utf8_bin'
    AND k.TABLE_NAME = c.TABLE_NAME COLLATE 'utf8_bin'
    AND k.COLUMN_NAME = c.COLUMN_NAME COLLATE 'utf8_bin'
LEFT JOIN information_schema.TABLES t
    ON t.TABLE_SCHEMA = c.TABLE_SCHEMA COLLATE 'utf8_bin'
    AND t.TABLE_NAME = c.TABLE_NAME COLLATE 'utf8_bin'
WHERE t.TABLE_TYPE = 'BASE TABLE'
    AND c.TABLE_SCHEMA NOT IN ('performance_schema',
                               'information_schema',
                               'mysql',
                               'sys',
                               'innodb')
ORDER BY c.TABLE_SCHEMA COLLATE 'utf8_bin',
         c.TABLE_NAME COLLATE 'utf8_bin',
         c.ORDINAL_POSITION;