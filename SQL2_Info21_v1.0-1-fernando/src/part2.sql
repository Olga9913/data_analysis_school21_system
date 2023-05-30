CREATE OR REPLACE PROCEDURE add_p2p_record(
    p_checker_name varchar,
    p_checked_name varchar,
    p_task_name varchar,
    p_check_status check_status,
    p_time timestamp DEFAULT current_timestamp
)
LANGUAGE plpgsql
AS $$
DECLARE
    l_check_id bigint;
BEGIN
    IF p_check_status = 'start' THEN
        INSERT INTO checks (id, peer, task, date)
        VALUES ((SELECT max(id) + 1
            AS ID FROM checks), p_checker_name, p_task_name, p_time)
        RETURNING id INTO l_check_id;
    ELSE
        SELECT max(id) + 1 INTO l_check_id
        FROM checks;
    END IF;

    INSERT INTO p2p (id, check_id, checking_peer, status, time)
    VALUES ((SELECT max(id) + 1
        AS id FROM p2p), l_check_id, p_checked_name, p_check_status, p_time);
END;
$$;


CALL add_p2p_record('warrynbo', 'carrowsh', 'SimpleBushUtils', 'failure');

-- Параметры: ник проверяемого, название задания, статус проверки Verter'ом, время.
-- Добавить запись в таблицу Verter (в качестве проверки указать проверку соответствующего задания с
-- самым поздним (по времени) успешным P2P этапом)
--

CREATE OR REPLACE PROCEDURE verter_check(
    p_nickname varchar,
    p_name_task varchar,
    p_check_status checkstatus,
    p_time timestamp
)
LANGUAGE plpgsql
AS $$
DECLARE
    l_check_id bigint := (SELECT checks.id
                         FROM p2p
                         JOIN checks ON checks.id = p2p.checkid
                         AND p2p.status = 'success'
                         WHERE checks.task = p_name_task
                         AND checks.peer = p_nickname
                         ORDER BY checks.task DESC, p2p.time DESC
                                  LIMIT 1);
BEGIN
    IF (l_check_id IS NOT NULL) THEN
        INSERT INTO verter (ID, checkid, status, time)
        VALUES ((SELECT max(ID) + 1
            AS ID FROM verter), l_check_id, p_check_status, p_time);
    END IF;
END;
$$;

CALL verter_check('carrowsh', 'SimpleBushUtils', 'success', '2023-04-20 13:23:54.000000');
