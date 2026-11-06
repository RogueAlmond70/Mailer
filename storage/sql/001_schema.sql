CREATE TABLE IF NOT EXISTS email_records (
    id text PRIMARY KEY,
    service text NOT NULL,
    from_address text NOT NULL,
    to_address text NOT NULL,
    subject text NOT NULL DEFAULT '',
    body text NOT NULL,
    attachment_filename text,
    attachment_content_type text,
    attachment_content bytea,
    attachment_content_id text,
    attachment_inline boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id bigserial PRIMARY KEY,
    request_id text NOT NULL,
    service text NOT NULL,
    from_address text NOT NULL,
    to_address text NOT NULL,
    subject text NOT NULL DEFAULT '',
    body text NOT NULL,
    attachment_filename text,
    attachment_content_type text,
    attachment_content bytea,
    attachment_content_id text,
    attachment_inline boolean NOT NULL DEFAULT false,
    failure_reason text NOT NULL,
    attempts integer NOT NULL,
    failed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dead_letters_request_id_idx ON dead_letters (request_id);
