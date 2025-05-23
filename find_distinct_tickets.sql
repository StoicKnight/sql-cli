WITH
-- Step 1: Find articles sent TO the target emails within the date range.
initial_target_articles AS (
    SELECT
        t.id AS ticket_id,
        a.id AS article_id,
        a.a_to AS target_email
    FROM
        otrs5.ticket t
    JOIN
        otrs5.article a
        ON t.id = a.ticket_id
    WHERE
        a.a_to IN ('whitelabel@hfm.com', 'info@hfm.com')
        AND t.create_time BETWEEN '2024-01-01' AND '2025-05-23'
),

-- Step 2: Count the TOTAL number of articles for ALL tickets.
all_ticket_article_counts AS (
    SELECT
        ticket_id,
        COUNT(*) AS total_article_count
    FROM
        otrs5.article
    GROUP BY
        ticket_id
),

-- Step 3: Combine the above
single_article_tickets_to_target AS (
    SELECT
        ita.ticket_id,
        ita.target_email
    FROM
        initial_target_articles ita
    JOIN
        all_ticket_article_counts atac
        ON ita.ticket_id = atac.ticket_id
    WHERE
        atac.total_article_count > 1
    GROUP BY ita.ticket_id, ita.target_email

)

-- Step 4: Count the "no-reply" tickets for each target email.
SELECT
    satt.target_email AS email,
    COUNT(satt.ticket_id) AS ticket_count
FROM
    single_article_tickets_to_target satt
GROUP BY
    satt.target_email;
