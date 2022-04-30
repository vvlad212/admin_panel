def get_all(time_select):
    get_all_data_from_postgre = f"""
    SELECT
    fw.modified,
    fw.id,
    fw.rating as imdb_rating,
    array_agg(DISTINCT g.name) filter (WHERE g.name is not null) as genre,
    fw.title,
    fw.description as description,
    coalesce(array_agg(DISTINCT p.full_name) filter (WHERE pfw.id is not null and pfw.role = 'director'), '{{}}'
           )     as director, 
    array_agg( DISTINCT p.full_name) filter (WHERE pfw.id is not null and pfw.role = 'actor') as actors_names,
    array_agg( DISTINCT p.full_name) filter (WHERE pfw.id is not null and pfw.role = 'writer') as writers_names,
    json_agg( DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name) ) 
    FILTER (WHERE p.id is not null and pfw.role = 'actor')as actors,
    json_agg( DISTINCT jsonb_build_object('id', p.id,'name', p.full_name)) 
    FILTER (WHERE p.id is not null and pfw.role = 'writer') as writers
    FROM content.film_work fw
             LEFT JOIN content.person_film_work pfw
                       ON pfw.film_work_id = fw.id
             LEFT JOIN content.person p ON p.id = pfw.person_id
             LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
             LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.modified > '{time_select}'
    GROUP BY fw.id
    ORDER BY fw.modified;
"""
    return get_all_data_from_postgre
