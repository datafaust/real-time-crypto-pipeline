-- create/reset readonly user (cluster-wide)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readonly') THEN
    CREATE ROLE readonly LOGIN PASSWORD 'readonly';
  ELSE
    ALTER ROLE readonly WITH PASSWORD 'readonly';
  END IF;
END$$;
