-- Local sample data only. The service does not run this file on startup.
-- Apply it with psql, or let docker-compose load it into a fresh database.

INSERT INTO email_records (
    id,
    service,
    from_address,
    to_address,
    subject,
    body
) VALUES
    (
        'seed-welcome',
        'band-builder',
        'mailer@example.com',
        'musician@example.com',
        'Welcome',
        '<p>Welcome to Band Builder.</p>'
    ),
    (
        'seed-reset',
        'band-builder',
        'mailer@example.com',
        'drummer@example.com',
        'Reset your password',
        '<p>Use this link to reset your password.</p>'
    )
ON CONFLICT (id) DO NOTHING;

INSERT INTO dead_letters (
    id,
    request_id,
    service,
    from_address,
    to_address,
    subject,
    body,
    failure_reason,
    attempts,
    failed_at
) VALUES (
    1,
    'seed-dlq-bounce',
    'band-builder',
    'mailer@example.com',
    'missing@example.com',
    'Gig reminder',
    '<p>Your gig is tomorrow.</p>',
    'maximum send attempts reached',
    10,
    '2026-01-15T12:00:00Z'
)
ON CONFLICT (id) DO NOTHING;

SELECT setval(
    pg_get_serial_sequence('dead_letters', 'id'),
    GREATEST((SELECT MAX(id) FROM dead_letters), 1)
);
