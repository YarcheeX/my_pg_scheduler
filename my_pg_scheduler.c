#include "postgres.h"
#include "fmgr.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "executor/spi.h"
#include "utils/timestamp.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/syslogger.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"

PG_MODULE_MAGIC;

/* Глобальные структуры */
static volatile sig_atomic_t got_sigterm = false;
static int scheduler_poll_interval = 60000; // 60 секунд по умолчанию

/* Прототипы функций */
static void scheduler_sigterm(SIGNAL_ARGS);
static void scheduler_sighup(SIGNAL_ARGS);
void scheduler_main(Datum main_arg);
static void process_pending_jobs(void);

/* Инициализация расширения */
void _PG_init(void) {
    BackgroundWorker worker;
    
    if (!process_shared_preload_libraries_in_progress)
        return;
    
    /* Настройка фонового воркера */
    memset(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_main = scheduler_main;
    worker.bgw_name = "pg-scheduler";
    worker.bgw_library_name = "pg_scheduler";
    worker.bgw_notify_pid = 0;
    
    RegisterBackgroundWorker(&worker);
}

/* Главная функция воркера */
void scheduler_main(Datum main_arg) {
    /* Блокировка сигналов для безопасной инициализации */
    BackgroundWorkerBlockSignals();
    
    /* Установка соединения с БД */
    if (!BackgroundWorkerInitializeConnection("postgres", NULL, 0)) {
        elog(ERROR, "scheduler: database connection failed");
        proc_exit(1);
    }
    
    BackgroundWorkerUnblockSignals();
    
    /* Настройка обработчиков сигналов */
    pqsignal(SIGTERM, scheduler_sigterm);
    pqsignal(SIGHUP, scheduler_sighup);
    pqsignal(SIGINT, StatementCancelHandler);
    pqsignal(SIGQUIT, quickdie);
    
    elog(LOG, "PostgreSQL Scheduler started");

    /* Основной рабочий цикл */
    while (!got_sigterm) {
        int rc;
    
        /* Ожидание события или таймаута */
        rc = WaitLatch(&MyProc->procLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                  scheduler_poll_interval,
                  WAIT_EVENT_BGWORKER_TIMEOUT);
    
            /* Сброс защелки для следующего цикла */
            ResetLatch(&MyProc->procLatch);
    
            /* Проверка смерти postmaster - критическая ошибка */
            if (rc & WL_POSTMASTER_DEATH) {
            got_sigterm = true;
            break; // Немедленный выход
        }
    
        /* Проверка прерываний (отмена запроса и т.д.) */
        CHECK_FOR_INTERRUPTS();
    
        /* Обработка перезагрузки конфигурации */
        if (ConfigReloadPending) {
            elog(LOG, "scheduler: reloading configuration");
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        
            /* Обновляем параметры */
            scheduler_poll_interval = 
                GetConfigOptionInt("scheduler.poll_interval", 60000, false);
        }
    
        /* Проверка режима восстановления */
        in_recovery = RecoveryInProgress();
    
        if (in_recovery) {
            elog(DEBUG1, "scheduler: in recovery mode, skipping job processing");
        } else {
            /* Обработка задач */
            process_pending_jobs();
        }
    
        /* Финальная проверка прерываний */
        CHECK_FOR_INTERRUPTS();
    
        /* Дополнительная точка выхода */
        if (got_sigterm) {
            break;
        }
    }
    
    elog(LOG, "PostgreSQL Scheduler shutting down");
    proc_exit(0);
}

/* Обработчик сигнала TERM */
static void scheduler_sigterm(SIGNAL_ARGS) {
    got_sigterm = true;
    SetLatch(&MyProc->procLatch);
}

/* Обработчик сигнала HUP (перезагрузка конфигурации) */
static void scheduler_sighup(SIGNAL_ARGS) {
    ConfigReloadPending = true;
    SetLatch(&MyProc->procLatch);
}

/* Обработка задач, готовых к выполнению */
static void process_pending_jobs(void) {
    int ret;
    uint64 i;
    
    /* Проверка прерываний перед началом транзакции */
    CHECK_FOR_INTERRUPTS();
    
    /* Начинаем новую транзакцию */
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    
    /* Подключаемся к SPI */
    if (SPI_connect() != SPI_OK_CONNECT) {
        elog(WARNING, "scheduler: SPI connection failed");
        PopActiveSnapshot();
        AbortCurrentTransaction();
        return;
    }
    
    /* Проверка перезагрузки конфигурации */
    if (ConfigReloadPending) {
        ConfigReloadPending = false;
        ProcessConfigFile(PGC_SIGHUP);
        
        /* Обновляем параметры */
        scheduler_poll_interval = 
            GetConfigOptionInt("scheduler.poll_interval", 60000, false);
    }
    
    /* Выбираем задачи, готовые к выполнению */
    ret = SPI_execute("SELECT job_id, command FROM scheduler.get_pending_jobs()", true, 0);
    
    if (ret != SPI_OK_SELECT) {
        elog(WARNING, "scheduler: failed to get pending jobs");
        SPI_finish();
        PopActiveSnapshot();
        AbortCurrentTransaction();
        return;
    }
    
    /* Обрабатываем каждую задачу */
    for (i = 0; i < SPI_processed; i++) {
        HeapTuple tuple = SPI_tuptable->vals[i];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        Datum job_id_datum, command_datum;
        bool job_id_isnull, command_isnull;
        int64 job_id;
        char *command = NULL;
        
        /* Проверка прерываний перед обработкой задачи */
        CHECK_FOR_INTERRUPTS();
        
        /* Извлекаем job_id */
        job_id_datum = SPI_getbinval(tuple, tupdesc, 1, &job_id_isnull);
        if (job_id_isnull) {
            elog(WARNING, "scheduler: job_id is NULL, skipping");
            continue;
        }
        job_id = DatumGetInt64(job_id_datum);
        
        /* Извлекаем команду */
        command_datum = SPI_getbinval(tuple, tupdesc, 2, &command_isnull);
        if (command_isnull) {
            elog(WARNING, "scheduler: command is NULL for job %ld, skipping", job_id);
            continue;
        }
        command = TextDatumGetCString(command_datum);
        
        elog(LOG, "scheduler: executing job %ld: %s", job_id, command);
        
        /* Создаем подтранзакцию для каждой задачи */
        BeginInternalSubTransaction("scheduler job");
        PG_TRY();
        {
            /* Проверка прерываний перед выполнением */
            CHECK_FOR_INTERRUPTS();
            
            /* Выполняем команду */
            int cmd_ret = SPI_execute(command, false, 0);
            if (cmd_ret < 0) {
                elog(WARNING, "scheduler: job %ld failed with code %d", job_id, cmd_ret);
            } else {
                /* Обновляем расписание */
                char *update_query = psprintf("SELECT scheduler.update_job_next_run(%ld)", job_id);
                SPI_execute(update_query, false, 0);
                pfree(update_query);
            }
            
            /* Фиксируем подтранзакцию */
            ReleaseCurrentSubTransaction();
        }
        PG_CATCH();
        {
            /* Откатываем подтранзакцию при ошибке */
            RollbackAndReleaseCurrentSubTransaction();
            elog(WARNING, "scheduler: job %ld failed: %s", job_id, CopyErrorData()->message);
            FlushErrorState();
        }
        PG_END_TRY();
        
        /* Освобождаем память */
        pfree(command);
        
        /* Периодическая проверка прерываний */
        if (i % 10 == 0) {
            CHECK_FOR_INTERRUPTS();
        }
    }
    
    /* Завершаем работу с SPI и транзакцией */
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}
