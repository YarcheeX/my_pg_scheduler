CREATE SCHEMA scheduler;

CREATE TABLE scheduler.jobs (
    job_id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    command TEXT NOT NULL,
    schedule_type TEXT NOT NULL CHECK (schedule_type IN ('CRON', 'INTERVAL', 'ONCE')),
    schedule_details TEXT NOT NULL,
    next_run TIMESTAMPTZ NOT NULL,
    last_run TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT TRUE,
    max_retries INT DEFAULT 0
);

CREATE INDEX jobs_next_run_idx ON scheduler.jobs (next_run) WHERE is_active;

CREATE FUNCTION scheduler.cron_next(cron_expr TEXT)
RETURNS TIMESTAMPTZ AS $$
DECLARE
    next_time TIMESTAMPTZ;
BEGIN
    -- Упрощенная реализация (для учебного проекта)
    -- В реальной реализации нужен полноценный парсер cron
    RETURN NOW() + '1 min'::INTERVAL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION scheduler.calculate_next_run(
    schedule_type TEXT,
    schedule_details TEXT,
    last_run TIMESTAMPTZ DEFAULT NULL
) RETURNS TIMESTAMPTZ AS $$
BEGIN
    IF schedule_type = 'CRON' THEN 
        RETURN scheduler.cron_next(schedule_details);
    ELSIF schedule_type = 'INTERVAL' THEN 
        RETURN COALESCE(last_run, NOW()) + schedule_details::INTERVAL;
    ELSIF schedule_type = 'ONCE' THEN 
        RETURN NULL;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION scheduler.get_pending_jobs()
RETURNS TABLE (job_id BIGINT, command TEXT) AS $$
BEGIN
    RETURN QUERY 
    SELECT j.job_id, j.command
    FROM scheduler.jobs j
    WHERE j.is_active
      AND j.next_run <= NOW()
      AND pg_try_advisory_lock(j.job_id)  -- Защита от параллельного выполнения
    FOR UPDATE SKIP LOCKED;  -- Избегаем блокировок
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION scheduler.update_job_next_run(job_id BIGINT)
RETURNS VOID AS $$
DECLARE
    job_record RECORD;
BEGIN
    SELECT * INTO job_record 
    FROM scheduler.jobs 
    WHERE job_id = $1;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job % not found', job_id;
    END IF;
    
    UPDATE scheduler.jobs 
    SET 
        last_run = NOW(),
        next_run = scheduler.calculate_next_run(
            schedule_type, 
            schedule_details, 
            NOW()
        )
    WHERE job_id = $1;
    
    -- Снимаем блокировку
    PERFORM pg_advisory_unlock(job_id);
END;
$$ LANGUAGE plpgsql;