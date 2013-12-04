-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###

-- arguments: id:string
SELECT
array_to_json(array_agg( concat_ws('.', m.major_version, m.minor_version) order by m.revised desc )) as versions 
FROM modules m
WHERE m.uuid = %(id)s
group by uuid;
