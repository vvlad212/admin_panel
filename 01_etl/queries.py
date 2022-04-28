get_all_data_from_postgre = """select * from (SELECT  
   fw.id,
   fw.rating as imdb_rating,
   array_agg(DISTINCT g.name) as genre,
   fw.title,
   fw.description,

       coalesce(
                       array_agg(
                       DISTINCT p.full_name
                   )
                       filter (WHERE pfw.role is not null and pfw.role = 'director')
           , '{}'
           )                      as director,


        array_agg(DISTINCT p.full_name )
            filter (where pfw.role = 'actor')
            as actors_names,

        array_agg(DISTINCT p.full_name )
        filter (where pfw.role = 'writer')
        as writers_names,


       COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id is not null and pfw.role = 'actor'),
       '[]'
   ) as actors,


           COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE p.id is not null and pfw.role = 'writer'),
       '[]'
   ) as writers

FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.modified < current_timestamp
and fw.id = '479f20b0-58d1-4f16-8944-9b82f5b1f22a'
GROUP BY fw.id
ORDER BY fw.modified) fppgg;
"""
