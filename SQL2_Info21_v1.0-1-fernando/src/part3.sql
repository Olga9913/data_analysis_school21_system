--  Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде
-- Ник пира 1, ник пира 2, количество переданных пир поинтов.
-- Количество отрицательное, если пир 2 получил от пира 1 больше поинтов.

-- Пример заполненной таблицы

-- VALUES (1, 'warrynbo', 'malindac', 5),
--         (2, 'malindac', 'carrowsh', 2),
--         (3, 'carrowsh', 'malindac', 4),
--         (4, 'chubumba', 'warrynbo', 1),
--         (5, 'warrynbo', 'chubumba', 1),
--         (6, 'dashilow', 'chubumba', 1);

DROP FUNCTION IF EXISTS transferred_points_human_readable();
CREATE OR REPLACE FUNCTION transferred_points_human_readable()
RETURNS TABLE (
        peer1 VARCHAR,
        peer2 VARCHAR,
        points_amount BIGINT
) AS $$
BEGIN
RETURN QUERY
SELECT checking_peer AS peer1, checked_peer AS peer2,
CASE
    WHEN
        (
         SELECT tp2.points_amount
         FROM transferred_points AS tp2
         WHERE checking_peer = tp1.checked_peer
           AND checked_peer = tp1.checking_peer
         ) > tp1.points_amount
    THEN
        tp1.points_amount * -1
    ELSE
        tp1.points_amount
    END
FROM transferred_points AS tp1;
END;
$$ LANGUAGE plpgsql;

-- Тестовый запрос.
SELECT *
FROM transferred_points_human_readable();


-- 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
-- В таблицу включать только задания, успешно прошедшие проверку (определять по таблице Checks).
-- Одна задача может быть успешно выполнена несколько раз. В таком случае в таблицу включать все успешные проверки.

CREATE OR REPLACE FUNCTION successfully_checked_tasks()
	RETURNS TABLE (
	    Peer VARCHAR,
	    Task VARCHAR,
	    XP BIGINT)
    AS $$
BEGIN
	RETURN QUERY (
SELECT checks.peer AS nickname, checks.task AS checked_task, xp.xp_amount AS got_xp
FROM xp
JOIN checks ON xp.check_id = checks.id
ORDER BY nickname
	);
END;
$$ LANGUAGE plpgsql;

-- Тестовый запрос.
SELECT *
FROM successfully_checked_tasks();

-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
-- Параметры функции: день, например 12.05.2022.
-- Функция возвращает только список пиров.

DROP FUNCTION IF EXISTS check_date(peer_date date);

CREATE OR REPLACE FUNCTION check_date(peer_date date)
    RETURNS TABLE
            (
                peer varchar
            )
AS
$$
SELECT peer AS login
FROM time_tracking
WHERE date = peer_date
GROUP BY peer
HAVING COUNT(
    CASE
    WHEN state = 2
        THEN 1
        END) = 0;

$$ LANGUAGE sql;

-- Тестовый запрос.
SELECT *
FROM check_date('2023-04-22');

-- 4) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
-- Результат вывести отсортированным по изменению числа поинтов.
-- Формат вывода: ник пира, изменение в количество пир поинтов

DROP PROCEDURE IF EXISTS peer_points_change(IN ref refcursor);

CREATE OR REPLACE PROCEDURE peer_points_change(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR

WITH checking AS (
    SELECT checking_peer AS peer, SUM(points_amount) AS points_change
    FROM transferred_points
    GROUP BY checking_peer
), checked AS (
    SELECT checked_peer AS peer, SUM(points_amount) AS points_change
    FROM transferred_points
    GROUP BY checked_peer
)
SELECT checking.peer, checking.points_change - COALESCE(checked.points_change, 0) AS points_change
FROM checking
LEFT JOIN checked ON checking.peer = checked.peer
WHERE checked.peer IS NULL
   OR (checking.peer IS NOT NULL AND checked.peer IS NOT NULL)
ORDER BY points_change DESC;
END;
$$ LANGUAGE plpgsql;

-- Тестовая транзакция.
BEGIN;
CALL peer_points_change('ref');
FETCH ALL IN "ref";
END;

-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3
-- Результат вывести отсортированным по изменению числа поинтов.
-- Формат вывода: ник пира, изменение в количество пир поинтов

