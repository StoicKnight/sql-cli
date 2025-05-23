SELECT COUNT(t.tn) AS 'Number of Tickets'
-- t.tn AS 'Ticket Number',
-- t.create_time AS 'Ticket Creation Timestamp',
-- t.title AS 'Ticket Title',
-- a.create_time AS 'Email Timestamp',
-- CONCAT('"', a.a_subject, '"') AS 'Email Subject',
-- CONCAT('"', a.a_body, '"') AS 'Email Content',
-- a.a_from AS 'Email From',
-- a.a_to AS 'Email To',
-- a.a_cc AS 'Email CC',
-- q.name AS 'Queue'
FROM otrs5.ticket t
INNER JOIN otrs5.queue q
    ON t.queue_id = q.id
INNER JOIN otrs5.users u
    ON t.user_id = u.id
INNER JOIN otrs5.article a
    ON t.id = a.ticket_id
WHERE
    q.name IN ('BO WC OGS', 'HFZA EV')
    AND
    t.ticket_state_id != 2
    AND
    t.title LIKE '%World Check%'
LIMIT 10
