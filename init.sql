CREATE UNLOGGED TABLE payments (
                          correlation_id UUID PRIMARY KEY,
                          amount DECIMAL NOT NULL,
                          processor TEXT NOT NULL,
                          requested_at TIMESTAMPTZ NOT NULL
);

-- Optional index for summary queries
CREATE INDEX idx_requested_at ON payments(requested_at);
create index idx_requested_at_processor ON payments(requested_at, processor);