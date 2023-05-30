CREATE TABLE peers (
    nickname varchar primary key,
    birthday date not null
);

INSERT INTO peers (nickname, birthday)
VALUES ('warrynbo', '1994-08-10'),
       ('carrowsh', '1995-09-11'),
       ('malindac', '1996-10-12'),
       ('dashilow', '1997-11-13'),
       ('chubumba', '1998-12-14'),
       ('derevyanko', '1999-01-15');

CREATE TABLE tasks (
    title       varchar primary key,
    parent_task varchar,
    max_xp      bigint not null default 0
);

INSERT INTO tasks (title, parent_task, max_xp)
VALUES ('SimpleBashUtils', null, 250),
       ('s21_string+', 'SimpleBashUtils', 500),
       ('s21_decimal', 's21_string+', 350),
       ('SmartCalc', 's21_decimal', 350),
       ('3DViewer', 'SmartCalc', 750);

CREATE TYPE check_status AS ENUM ('start', 'success', 'failure');

CREATE TABLE checks (
    id   bigint primary key,
    peer varchar   not null,
    task varchar   not null,
    date timestamp not null
);

INSERT INTO checks (id, peer, task, date)
VALUES (1, 'malindac', 'SimpleBashUtils', '2023-04-20 13:00:00'),
       (2, 'warrynbo', 'SimpleBashUtils', '2023-04-21 14:45:00'),
       (3, 'carrowsh', 'SimpleBashUtils', '2023-04-22 16:00:00'),
       (4, 'dashilow', 'SimpleBashUtils', '2023-04-23 11:30:00'),
       (5, 'warrynbo', 's21_string+', '2023-04-24 10:30:00');

CREATE TABLE p2p (
    id            bigint primary key,
    check_id      bigint    not null,
    checking_peer varchar   not null,
    status        check_status,
    time          timestamp not null,
    constraint fk_p2p_check_id foreign key (check_id) references checks (id)
);

INSERT INTO p2p (id, check_id, checking_peer, status, time)
VALUES (1, 1, 'warrynbo', 'start', '2023-04-20 13:00:00'),
       (2, 1, 'warrynbo', 'success', '2023-04-20 13:23:54'),
       (3, 2, 'malindac', 'start', '2023-04-21 14:45:00'),
       (4, 2, 'malindac', 'success', '2023-04-21 15:10:54'),
       (5, 3, 'dashilow', 'start', '2023-04-22 16:00:00'),
       (6, 3, 'dashilow', 'failure', '2023-04-22 16:29:54'),
       (7, 4, 'carrowsh', 'start', '2023-04-23 11:30:00'),
       (8, 4, 'carrowsh', 'failure', '2023-04-23 11:45:54'),
       (9, 5, 'chubumba', 'start', '2023-04-24 10:30:00'),
       (10, 5, 'chubumba', 'success', '2023-04-24 10:59:54');

CREATE TABLE verter (
    id       bigint primary key,
    check_id bigint    not null,
    status   check_status,
    time     timestamp not null,
    constraint fk_verter_CheckID foreign key (check_id) references checks (id)
);

INSERT INTO verter (id, check_id, status, time)
VALUES (1, 1, 'start', '2023-04-20 13:24:00'),
       (2, 1, 'success', '2023-04-20 13:25:00'),
       (3, 2, 'start', '2023-04-21 15:11:00'),
       (4, 2, 'success', '2023-04-21 15:12:00'),
       (5, 5, 'start', '2023-04-24 11:00:00'),
       (6, 5, 'failure', '2023-04-24 11:01:00');

CREATE TABLE transferred_points (
    id            bigint primary key,
    checking_peer varchar not null,
    checked_peer  varchar not null,
    points_amount bigint  not null
);

INSERT INTO transferred_points (id, checking_peer, checked_peer, points_amount)
VALUES (1, 'warrynbo', 'malindac', 1),
       (2, 'malindac', 'warrynbo', 1),
       (3, 'dashilow', 'carrowsh', 1),
       (4, 'carrowsh', 'dashilow', 1),
       (5, 'chubumba', 'warrynbo', 1);

CREATE TABLE friends (
    id    bigint primary key,
    peer1 varchar not null,
    peer2 varchar not null
);

INSERT INTO friends (id, peer1, peer2)
VALUES (1, 'warrynbo', 'malindac'),
       (2, 'dashilow', 'carrowsh'),
       (3, 'malindac', 'chubumba'),
       (4, 'carrowsh', 'derevyanko'),
       (5, 'chubumba', 'warrynbo');

CREATE TABLE recommendations (
    id               bigint primary key,
    peer             varchar not null,
    recommended_peer varchar not null
);

INSERT INTO recommendations (id, peer, recommended_peer)
VALUES (1, 'warrynbo', 'carrowsh'),
       (2, 'malindac', 'dashilow'),
       (3, 'dashilow', 'chubumba'),
       (4, 'derevyanka', 'carrowsh'),
       (5, 'chubumba', 'warrynbo');

CREATE TABLE xp (
    id        bigint primary key,
    check_id  bigint not null,
    xp_amount bigint not null default 0,
    CONSTRAINT ch_xp_amount CHECK (xp_amount >= 0)
);

INSERT INTO xp (id, check_id, xp_amount)
VALUES (1, 1, 250),
       (2, 2, 250);

CREATE TABLE time_tracking (
    id    bigint primary key,
    peer  varchar  not null,
    date  date     not null,
    time  time     not null,
    state smallint not null default 2,
    CONSTRAINT ch_state CHECK ( State in (1, 2) )
);

INSERT INTO time_tracking (id, peer, date, time, state)
VALUES (1, 'warrynbo', '2023-04-20', '12:05', 1),
       (2, 'warrynbo', '2023-04-20', '16:23', 2),
       (3, 'malindac', '2023-04-21', '14:31', 1),
       (4, 'malindac', '2023-04-21', '18:14', 2),
       (5, 'dashilow', '2023-04-22', '15:47', 1),
       (6, 'dashilow', '2023-04-22', '21:02', 2);

CREATE OR REPLACE PROCEDURE import_from_csv(table_name TEXT, file_path TEXT, delimiter TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('COPY %I FROM %L WITH DELIMITER %L HEADER CSV;', table_name, file_path, delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE export_to_csv(table_name TEXT, file_path TEXT, delimiter TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('COPY %I TO %L WITH DELIMITER %L HEADER CSV;', table_name, file_path, delimiter);
END;
$$;

CALL import_from_csv('friends', '/home/olga/school21/sql/SQL2_Info21_v1.0-1/src/part1/friends2.csv', ',');

-- \copy TimeTracking TO '/home/olga/school21/sql/SQL2_Info21_v1.0-1/src/part1/TimeTracking.csv' WITH (FORMAT CSV, DELIMITER ',', HEADER)

CALL export_to_csv('friends','/home/olga/school21/sql/SQL2_Info21_v1.0-1/src/part1/friends.csv',',');
