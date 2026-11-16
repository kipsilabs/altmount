-- +goose Up
-- parts is the exact part list verified at accept for the file of a bundle
-- import: the segment count of each part, comma separated ("3,5,4"). The
-- empty string means one posted file (no parts). NULL means the row was
-- written before this column existed and carries no boundary knowledge; the
-- forced full check then runs only the checks that do not need boundaries.
ALTER TABLE peernet_bundle_imports ADD COLUMN parts TEXT;

-- +goose Down
ALTER TABLE peernet_bundle_imports DROP COLUMN parts;
