USE [master]
GO
EXECUTE msdb.dbo.rds_set_system_database_sync_objects @object_types = 'SQLAgentJob';
GO

CREATE DATABASE [crdrdb]
GO

USE [crdrdb]
GO
/****** Object:  StoredProcedure [dbo].[uspManagePrimaryLSBackupCopy]    Script Date: 11/19/2022 11:46:52 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[uspManagePrimaryLSBackupCopy]
(
		@DB_Name NVARCHAR(400) -- Enter your database name
		,@KMS_Key_ARN NVARCHAR(4000) -- Enter your Multi-Region KMS Key ARN
		,@source_server CHAR(100) -- Enter your Production instance endpoint name
		,@target_server CHAR(100) -- Enter your DR instance endpoint name	

)
/***********Author - Rajib Sadhu**************/

AS
BEGIN
		SET NOCOUNT ON

			DECLARE @min_seq_id INT
			DECLARE @max_seq_id INT

			SELECT	@min_seq_id = MIN([rds_backup_seq_id]) 
			FROM	[TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] 
			WHERE	[source_server] = @source_server 
			AND		[target_server] = @target_server 
			AND		[database_name] = @DB_Name 
			AND		[backup_type] = 'Log'
			AND		[processing_status] IS NULL

			SELECT	@max_seq_id = MAX([rds_backup_seq_id]) 
			FROM	[TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] 
			WHERE	[source_server] = @source_server 
			AND		[target_server] = @target_server 
			AND		[database_name] = @DB_Name 
			AND		[backup_type] = 'Log'
			AND		[processing_status] IS NULL
			
			EXEC msdb.dbo.rds_tlog_backup_copy_to_S3 @db_name = @DB_Name, @rds_backup_starting_seq_id = @min_seq_id, @rds_backup_ending_seq_id = @max_seq_id , @kms_key_arn= @KMS_Key_ARN

			IF(@@ERROR = 0)
			BEGIN

				UPDATE	[TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] 
				SET		[processing_status] = 'copy-in-progress'
				WHERE	[source_server] = @source_server 
				AND		[target_server] = @target_server 
				AND		[database_name] = @DB_Name 
				AND		[backup_type] = 'Log'
				AND		[processing_status] IS NULL
				AND		[rds_backup_seq_id] BETWEEN @min_seq_id AND @max_seq_id

			END


		SET NOCOUNT OFF
END
GO
/****** Object:  StoredProcedure [dbo].[uspManagePrimaryLSTracking]    Script Date: 11/19/2022 11:46:52 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[uspManagePrimaryLSTracking]
(
		@ListofDBs NVARCHAR(MAX) -- Enter your database name as comma separated
		,@source_server CHAR(100) -- Enter your Production instance endpoint name
		,@target_server CHAR(100) -- Enter your DR instance endpoint name	
)
/***********Author - Rajib Sadhu**************/
/*
EXEC dbo.uspManagePrimaryLSTracking 'AdventureWorks2019,AdventureWorksDW2019', 'rds-sql-server-crdr-us-west-2-instance', 'rds-sql-server-crdr-us-east-2-instance'
*/
AS
BEGIN		
	SET NOCOUNT ON

	DECLARE @cmd NVARCHAR(1000)
	DECLARE @LiCounter INT = 1
	DECLARE @LiMaxCount INT
	DECLARE @LsDatabaseName SYSNAME
	DECLARE @FullBackupLastLSN NUMERIC(25, 0)
	DECLARE @BackupSequenceId INT

	/*****************Parse the comma separated database names****************/

	CREATE TABLE #DBList
	(
		DatabaseId	INT IDENTITY(1,1),
		DatabaseName SYSNAME
	)

	DECLARE @DbListXML XML = CAST('<root><U>'+ Replace(@ListofDBs, ',', '</U><U>')+ '</U></root>' AS XML)
    
	INSERT INTO #DBList (DatabaseName)

	SELECT f.x.value('.', 'SYSNAME') AS user_id
	FROM @DbListXML.nodes('/root/U') f(x)
    

	/************Populate Full Backup and Tran Log information*****************/

	SET @LiCounter = 1

	WHILE(1=1)
	BEGIN
			SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

			IF(@LiCounter <= @LiMaxCount)
			BEGIN
					SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter
						
					---Populate full backup information

					IF NOT EXISTS(SELECT 1 FROM [TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] WHERE [source_server] = @source_server AND [target_server] = @target_server AND [database_name] = @LsDatabaseName AND [backup_type] = 'Database')
					BEGIN
					
						INSERT INTO [TrackingDBServer].[trackingdb].[dbo].[tblLSTracking]
								   ([source_server]
								   ,[target_server]
								   ,[database_name]
								   ,[backup_start_date]
								   ,[backup_finish_date]
								   ,[backup_type]
								   ,[backup_size]
								   ,[physical_device_name]
								   ,[file_name]
								   ,[backupset_name]
								   ,[processing_status]
								   ,[backup_set_id]
								   ,[checkpoint_lsn]
								   ,[database_backup_lsn]
								   ,[last_lsn])


						SELECT		TOP 1
									@source_server, 
									@target_server,
									bs.database_name, 
									bs.backup_start_date, 
									bs.backup_finish_date, 
									CASE	bs.type 
										WHEN 'D' THEN 'Database' 
										WHEN 'L' THEN 'Log' 
										END AS backup_type, 
									bs.backup_size, 
									bmf.physical_device_name, 
									bs.database_name + '_fullbackup.bak',
									bs.name as backupset_name,
									NULL,
									bs.backup_set_id,
									bs.checkpoint_lsn,
									bs.database_backup_lsn,
									bs.last_lsn

						FROM		msdb.dbo.backupmediafamily bmf
						INNER JOIN	msdb.dbo.backupset bs
						ON			bmf.media_set_id = bs.media_set_id 
						WHERE		bs.database_name = @LsDatabaseName
						AND			bs.type = 'D'
						AND			bs.is_snapshot = 0
						ORDER BY	bs.backup_finish_date DESC

					END

					
					--Populate tran log backup information

					SELECT @FullBackupLastLSN = [last_lsn] FROM [TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] WHERE [source_server] = @source_server AND [target_server] = @target_server AND [database_name] = @LsDatabaseName AND [backup_type] = 'Database'

					SELECT @BackupSequenceId = MAX([rds_backup_seq_id]) FROM [TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] WHERE [source_server] = @source_server AND [target_server] = @target_server AND [database_name] = @LsDatabaseName AND [backup_type] = 'Log'
									   					
					INSERT INTO [TrackingDBServer].[trackingdb].[dbo].[tblLSTracking]
								  ([source_server]
								   ,[target_server]
								   ,[database_name]
								   ,[backup_start_date]
								   ,[backup_finish_date]
								   ,[backup_type]
								   ,[backup_size]
								   ,[physical_device_name]
								   ,[file_name]
								   ,[backupset_name]
								   ,[processing_status]
								   ,[backup_set_id]
								   ,[checkpoint_lsn]
								   ,[database_backup_lsn]
								   ,[db_id]
								   ,[family_guid]
								   ,[rds_backup_seq_id]
								   ,[backup_file_epoch]
								   ,[starting_lsn]
								   ,[ending_lsn])

					SELECT 
								@source_server, 
								@target_server,
								TLOGBM.[db_name], 
								NULL, 
								TLOGBM.backup_file_time_utc, 
								'Log' AS backup_type, 
								TLOGBM.file_size_bytes, 
								LTRIM(STR(TLOGBM.[db_id])) + '.' + LOWER(TLOGBM.family_guid), 
								LTRIM(STR(TLOGBM.[db_id])) + '.' + LOWER(TLOGBM.family_guid) + '.' + LTRIM(STR(TLOGBM.rds_backup_seq_id)) + '.' + STR(TLOGBM.backup_file_epoch),
								NULL,
								NULL,
								NULL,
								NULL,
								NULL,
								TLOGBM.[db_id],
								TLOGBM.[family_guid],
								TLOGBM.[rds_backup_seq_id],
								TLOGBM.[backup_file_epoch],
								TLOGBM.[starting_lsn],
								TLOGBM.[ending_lsn]
	

					FROM		msdb.dbo.rds_fn_list_tlog_backup_metadata(@LsDatabaseName) TLOGBM	
					WHERE		((@BackupSequenceId IS NULL AND @FullBackupLastLSN BETWEEN TLOGBM.starting_lsn AND TLOGBM.ending_lsn)
														OR
								(@BackupSequenceId IS NOT NULL AND TLOGBM.rds_backup_seq_id > @BackupSequenceId))
					ORDER BY	TLOGBM.rds_backup_seq_id ASC	
				


					SET @LiCounter = @LiCounter +1
			END
			ElSE
			BEGIN
					BREAK
			END

	END --WHILE(1=1)


	DROP TABLE #DBList

	SET NOCOUNT OFF
END
GO
/****** Object:  StoredProcedure [dbo].[uspManagePrimarySetLogShipping]    Script Date: 11/19/2022 11:46:52 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[uspManagePrimarySetLogShipping]
(
		@ListofDBs NVARCHAR(MAX) -- Enter your database name as comma separated
		,@LogBackupFrequency SMALLINT --Enter how frequently you want to backup tran logs
		,@PrimaryServerAdminUser NVARCHAR(100) -- Enter Primary Server Admin user name
		,@KMS_Key_ARN NVARCHAR(4000) -- Enter your Multi-Region KMS Key ARN
		,@source_server CHAR(100) -- Enter your Production instance endpoint name
		,@target_server CHAR(100) -- Enter your DR instance endpoint name	

)
/***********Author - Rajib Sadhu**************/

AS
BEGIN
		SET NOCOUNT ON
		
		DECLARE @cmd NVARCHAR(4000)
		DECLARE @LiCounter INT = 1
		DECLARE @LiMaxCount INT
		DECLARE @LsDatabaseName SYSNAME
		DECLARE @JobName NVARCHAR(1000)
		DECLARE @jobId BINARY(16)
		DECLARE @LSBackUpScheduleUID   UNIQUEIDENTIFIER 
		DECLARE @LSBackUpScheduleID    INT 
		
		/*****************Parse the comma separated database names****************/

		CREATE TABLE #DBList
		(
			DatabaseId	INT IDENTITY(1,1),
			DatabaseName SYSNAME
		)

		DECLARE @DbListXML XML = CAST('<root><U>'+ Replace(@ListofDBs, ',', '</U><U>')+ '</U></root>' AS XML)
    
		INSERT INTO #DBList (DatabaseName)

		SELECT f.x.value('.', 'SYSNAME') AS user_id
		FROM @DbListXML.nodes('/root/U') f(x)
    
		/***************Make sure DB List does not have any system databases***********************/
		IF EXISTS(SELECT 1 FROM #DBList WHERE DatabaseName IN ('master','model','msdb','tempdb','rdsadmin','ssisdb','crdrdb'))
		BEGIN
			RAISERROR('Please remove system database/s from the list to proceed.', 16, 1)
			RETURN 1
		END

		/****************Check if the database specified exists***********************************/
		IF EXISTS (SELECT DatabaseName from #DBList WHERE DatabaseName NOT IN (SELECT name FROM master.sys.databases))
		BEGIN
		RAISERROR('One of more databases in the list are not valid. Supply valid database name/s. To see available databases, use sys.databases.', 16, 1)
		RETURN 1
		END

		/*****************Check if the database is not online*************************************/

		WHILE(1=1)
		BEGIN
				SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

				IF(@LiCounter <= @LiMaxCount)
				BEGIN
						SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter
						
						IF (DATABASEPROPERTYEX(@LsDatabaseName, N'STATUS') != N'ONLINE')
						BEGIN
						RAISERROR(32008, 10, 1, @LsDatabaseName)
						END

						SET @LiCounter = @LiCounter +1
				END
				ElSE
				BEGIN
						BREAK
				END

		END --WHILE(1=1)

		--/********Check if logshipping entry for this database already exists*****************/

		--IF EXISTS(SELECT 1 FROM #DBList WHERE DatabaseName IN (SELECT primary_database FROM msdb.dbo.log_shipping_primary_databases))
		--BEGIN
		--	RAISERROR('One or more databases in the list already configured in Log Shipping. Please remove database/s from the list to proceed.', 16, 1)
		--	RETURN 1
		--END
			     

		


		
		/*******************************Create Log Backup Jobs******************************/

		SET @LiCounter = 1

		WHILE(1=1)
		BEGIN
				SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

				IF(@LiCounter <= @LiMaxCount)
				BEGIN
						SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter

						SET @JobName = '_LSBackup_' + @LsDatabaseName					
									
						SET @cmd = 'EXEC [crdrdb].[dbo].[uspManagePrimaryLSBackupCopy]  ' + '''' + @LsDatabaseName + '''' + ' ,' + '''' + LTRIM(RTRIM(@KMS_Key_ARN)) + '''' + ' ,' + '''' + LTRIM(RTRIM(@source_server)) + '''' + ' ,' +  '''' + LTRIM(RTRIM(@target_server)) + '''' + ';'

						SET @jobId = NULL
								
						EXEC  msdb.dbo.sp_add_job @job_name=@JobName, 
								@enabled=1, 
								@notify_level_eventlog=0, 
								@notify_level_email=2, 
								@notify_level_page=2, 
								@delete_level=0, 
								@category_name=N'[Uncategorized (Local)]', 
								@owner_login_name=@PrimaryServerAdminUser, @job_id = @jobId OUTPUT

						EXEC msdb.dbo.sp_add_jobserver @job_name=@JobName, @server_name = @@SERVERNAME

						EXEC msdb.dbo.sp_add_jobstep @job_name=@JobName, @step_name=N'LS Backup Copy', 
								@step_id=1, 
								@cmdexec_success_code=0, 
								@on_success_action=1, 
								@on_fail_action=2, 
								@retry_attempts=0, 
								@retry_interval=0, 
								@os_run_priority=0, @subsystem=N'TSQL', 
								@command=@cmd, 
								@database_name=N'master', 
								@flags=0		
							
						EXEC msdb.dbo.sp_add_schedule 
								@schedule_name =N'LS Backup Copy Schedule' 
								,@enabled = 1 
								,@freq_type = 4 
								,@freq_interval = 1 
								,@freq_subday_type = 4 
								,@freq_subday_interval = @LogBackupFrequency 
								,@freq_recurrence_factor = 0 
								,@active_start_date = 20100101 
								,@active_end_date = 99991231 
								,@active_start_time = 0 
								,@active_end_time = 235900 
								,@schedule_uid = @LSBackUpScheduleUID OUTPUT 
								,@schedule_id = @LSBackUpScheduleID OUTPUT 

						EXEC msdb.dbo.sp_attach_schedule 
								@job_id = @jobId 
								,@schedule_id = @LSBackUpScheduleID						

						SET @LiCounter = @LiCounter +1
				END
				ElSE
				BEGIN
						BREAK
				END

		END --WHILE(1=1)

			
		DROP TABLE #DBList

		SET NOCOUNT OFF
END
GO
/****** Object:  StoredProcedure [dbo].[uspManagePrimarySetPrimary]    Script Date: 11/19/2022 11:46:52 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[uspManagePrimarySetPrimary]
(
		@ListofDBs NVARCHAR(MAX) -- Enter your database name as comma separated
		,@PrimaryServerS3BucketARN NVARCHAR(500) -- Pass the S3 Bucket ARN of the Primary SQL Server
		,@PrimaryServerAdminUser NVARCHAR(100) -- Enter Primary Server Admin user name
		,@PrimaryServer CHAR(100) -- Enter your Production instance endpoint name
		,@SecondaryServer CHAR(100) -- Enter your DR instance endpoint name	
		
)
/***********Author - Rajib Sadhu**************/

AS
BEGIN
		SET NOCOUNT ON

		DECLARE @cmd NVARCHAR(4000)
		DECLARE @LiCounter INT = 1
		DECLARE @LiMaxCount INT
		DECLARE @LsDatabaseName SYSNAME
		DECLARE @jobId BINARY(16)
		DECLARE @JobName NVARCHAR(1000)
		DECLARE @BackupPath NVARCHAR(4000)
		DECLARE @LSBackUpScheduleUID   UNIQUEIDENTIFIER 
		DECLARE @LSBackUpScheduleID    INT 

		/*****************Parse the comma separated database names****************/

		CREATE TABLE #DBList
		(
			DatabaseId	INT IDENTITY(1,1),
			DatabaseName SYSNAME
		)

		DECLARE @DbListXML XML = CAST('<root><U>'+ Replace(@ListofDBs, ',', '</U><U>')+ '</U></root>' AS XML)
    
		INSERT INTO #DBList (DatabaseName)

		SELECT f.x.value('.', 'SYSNAME') AS user_id
		FROM @DbListXML.nodes('/root/U') f(x)
    
		/***************Make sure DB List does not have any system databases***********************/
		IF EXISTS(SELECT 1 FROM #DBList WHERE DatabaseName IN ('master','model','msdb','tempdb','rdsadmin','ssisdb','crdrdb'))
		BEGIN
			RAISERROR('Please remove system database/s from the list to proceed.', 16, 1)
			RETURN 1
		END

		/****************Check if the database specified exists***********************************/
		IF EXISTS (SELECT DatabaseName from #DBList WHERE DatabaseName NOT IN (SELECT name FROM master.sys.databases))
		BEGIN
		RAISERROR('One of more databases in the list are not valid. Supply valid database name/s. To see available databases, use sys.databases.', 16, 1)
		RETURN 1
		END

		/*****************Check if the database is not online*************************************/

		WHILE(1=1)
		BEGIN
				SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

				IF(@LiCounter <= @LiMaxCount)
				BEGIN
						SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter
						
						IF (DATABASEPROPERTYEX(@LsDatabaseName, N'STATUS') != N'ONLINE')
						BEGIN
						RAISERROR(32008, 10, 1, @LsDatabaseName)
						END

						SET @LiCounter = @LiCounter +1
				END
				ElSE
				BEGIN
						BREAK
				END

		END --WHILE(1=1)		


		/****************Change database recovery model to Full****************************************/

		SET @LiCounter = 1

		WHILE(1=1)
		BEGIN
				SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

				IF(@LiCounter <= @LiMaxCount)
				BEGIN
						SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter
						
						IF (DATABASEPROPERTYEX(@LsDatabaseName, N'Recovery') != N'FULL')
						BEGIN
								SET @cmd = 'ALTER DATABASE ' + @LsDatabaseName + ' SET RECOVERY FULL'
								EXEC (@cmd)
						END

						SET @LiCounter = @LiCounter +1
				END
				ElSE
				BEGIN
						BREAK
				END

		END --WHILE(1=1)

		
		/*******************************Create Full Backup Jobs******************************/
		
		SET @LiCounter = 1

		WHILE(1=1)
		BEGIN
				SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

				IF(@LiCounter <= @LiMaxCount)
				BEGIN
						SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter

						SET @JobName = '_FullBackup_' + @LsDatabaseName
						
						SET @BackupPath = '''' + LOWER(@PrimaryServerS3BucketARN) + '/' + LOWER(@LsDatabaseName) + '_fullbackup.bak' + ''''
						
						SET @cmd = 'exec msdb.dbo.rds_backup_database @source_db_name=' + '''' +  @LsDatabaseName + '''' + ', @s3_arn_to_backup_to=' + @BackupPath + ', @type=''FULL'', @overwrite_s3_backup_file=1;'

						SET @jobId = NULL
								
						EXEC  msdb.dbo.sp_add_job @job_name=@JobName, 
								@enabled=1, 
								@notify_level_eventlog=0, 
								@notify_level_email=2, 
								@notify_level_page=2, 
								@delete_level=0, 
								@category_name=N'[Uncategorized (Local)]', 
								@owner_login_name=@PrimaryServerAdminUser, @job_id = @jobId OUTPUT
														
						EXEC msdb.dbo.sp_add_jobserver @job_name=@JobName, @server_name = @@SERVERNAME

						EXEC msdb.dbo.sp_add_jobstep @job_name=@JobName, @step_name=N'full backup', 
								@step_id=1, 
								@cmdexec_success_code=0, 
								@on_success_action=1, 
								@on_fail_action=2, 
								@retry_attempts=0, 
								@retry_interval=0, 
								@os_run_priority=0, @subsystem=N'TSQL', 
								@command=@cmd, 
								@database_name=N'master', 
								@flags=0											

						SET @LiCounter = @LiCounter +1
				END
				ElSE
				BEGIN
						BREAK
				END

		END --WHILE(1=1)


		/*******************************Create LS Tracking Job******************************/

		SET @JobName = '_LSTracking'
		
		SET @cmd = 'EXEC [crdrdb].[dbo].[uspManagePrimaryLSTracking] ' + '''' + CAST(@ListofDBs AS NVARCHAR(4000)) + '''' + ', ' + '''' + LTRIM(RTRIM(@PrimaryServer)) + '''' + ', ' + '''' + LTRIM(RTRIM(@SecondaryServer)) + ''''

		SET @jobId = NULL
		SET @LSBackUpScheduleUID   = NULL 
		SET @LSBackUpScheduleID    = NULL 
																
		EXEC  msdb.dbo.sp_add_job @job_name=@JobName, 
				@enabled=0, 
				@notify_level_eventlog=0, 
				@notify_level_email=2, 
				@notify_level_page=2, 
				@delete_level=0, 
				@category_name=N'[Uncategorized (Local)]', 
				@owner_login_name=@PrimaryServerAdminUser, @job_id = @jobId OUTPUT

		EXEC msdb.dbo.sp_add_jobserver @job_name=@JobName, @server_name = @@SERVERNAME

		EXEC msdb.dbo.sp_add_jobstep @job_name=@JobName, @step_name=N'LS Tracking', 
				@step_id=1, 
				@cmdexec_success_code=0, 
				@on_success_action=1, 
				@on_fail_action=2, 
				@retry_attempts=0, 
				@retry_interval=0, 
				@os_run_priority=0, @subsystem=N'TSQL', 
				@command=@cmd, 
				@database_name=N'master', 
				@flags=0		
							
		EXEC msdb.dbo.sp_add_schedule 
				@schedule_name =N'LSTrackingSchedule' 
				,@enabled = 1 
				,@freq_type = 4 
				,@freq_interval = 1 
				,@freq_subday_type = 4 
				,@freq_subday_interval = 5 
				,@freq_recurrence_factor = 0 
				,@active_start_date = 20100101 
				,@active_end_date = 99991231 
				,@active_start_time = 0 
				,@active_end_time = 235900 
				,@schedule_uid = @LSBackUpScheduleUID OUTPUT 
				,@schedule_id = @LSBackUpScheduleID OUTPUT 

		EXEC msdb.dbo.sp_attach_schedule 
				@job_id = @jobId 
				,@schedule_id = @LSBackUpScheduleID		

		DROP TABLE #DBList

		SET NOCOUNT OFF
END
GO
GO
/****** Object:  StoredProcedure [dbo].[uspManageSecondaryLSTracking]    Script Date: 11/19/2022 11:48:18 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[uspManageSecondaryLSTracking]
(
		@ListofDBs NVARCHAR(MAX) -- Enter your database name as comma separated
		,@source_server CHAR(100) -- Enter your Production instance endpoint name
		,@target_server CHAR(100) -- Enter your DR instance endpoint name
	
)
/***********Author - Rajib Sadhu**************/
/*
EXEC dbo.uspManageSecondaryLSTracking 'AdventureWorks2019,AdventureWorksDW2019', 'rds-sql-server-crdr-us-west-2-instance', 'rds-sql-server-crdr-us-east-2-instance'
*/
AS
BEGIN		
	SET NOCOUNT ON

	DECLARE @cmd NVARCHAR(1000)
	DECLARE @LiCounter INT = 1
	DECLARE @LiMaxCount INT
	DECLARE @LsDatabaseName SYSNAME
	DECLARE @FullBackupCheckpointLSN NUMERIC(25, 0)
	DECLARE @BackupSetId INT

	/*****************Parse the comma separated database names****************/

	CREATE TABLE #DBList
	(
		DatabaseId	INT IDENTITY(1,1),
		DatabaseName SYSNAME
	)

	DECLARE @DbListXML XML = CAST('<root><U>'+ Replace(@ListofDBs, ',', '</U><U>')+ '</U></root>' AS XML)
    
	INSERT INTO #DBList (DatabaseName)

	SELECT f.x.value('.', 'SYSNAME') AS user_id
	FROM @DbListXML.nodes('/root/U') f(x)
    

	/************Populate Full Backup and Tran Log information*****************/

	
	CREATE TABLE #TblTaskStatus
	(
			task_id	int,
			task_type varchar(250),
			[database_name] varchar(500),	
			[complete] numeric(5,2),
			[duration_mins] int,
			lifecycle varchar(250),
			task_info varchar(max),
			last_updated datetime,	
			created_at datetime,
			S3_object_arn varchar(8000),
			overwrite_S3_backup_file varchar(50),	
			KMS_master_key_arn varchar(500),
			filepath varchar(500),
			overwrite_file varchar(500)
	)


	SET @LiCounter = 1

	WHILE(1=1)
	BEGIN
			SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

			IF(@LiCounter <= @LiMaxCount)
			BEGIN
					SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter
						
					---Populate tran log restore status
					
					INSERT INTO #TblTaskStatus
					EXEC msdb.dbo.rds_task_status @LsDatabaseName
						
					UPDATE		LST
					SET			LST.processing_status = 'Processed'

					FROM		[TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] LST
					INNER JOIN	#TblTaskStatus TS
					ON			TS.database_name = LST.database_name
					AND			TS.task_type = 'RESTORE_DB_LOG_NORECOVERY'
					AND			LST.database_name = @LsDatabaseName
					AND			LST.backup_type = 'Log'
					AND			TS.lifecycle = 'SUCCESS'
					AND			LST.[source_server] = @source_server 
					AND			LST.[target_server] = @target_server 
					AND			LST.file_name = SUBSTRING(TS.S3_object_arn, (LEN(TS.S3_object_arn) - CHARINDEX('/', REVERSE(TS.S3_object_arn)) +2), LEN(TS.S3_object_arn))						
					AND			LST.processing_status = 'restore-in-progress'

			
					DELETE FROM #TblTaskStatus
							
					SET @LiCounter = @LiCounter +1

			END
			ElSE
			BEGIN
					BREAK
			END

	END --WHILE(1=1)


	DROP TABLE #DBList
	DROP TABLE #TblTaskStatus

	SET NOCOUNT OFF
END
GO
/****** Object:  StoredProcedure [dbo].[uspManageSecondaryRestoreLogs]    Script Date: 11/19/2022 11:48:18 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[uspManageSecondaryRestoreLogs]
(
		@DatabaseName NVARCHAR(128) -- Enter your database name as comma separated
		,@SecondaryServerS3BucketARN NVARCHAR(500) -- Pass the Secondary Server S3 Bucket ARN
		,@KMS_Key_ARN NVARCHAR(4000) -- Enter your Multi-Region KMS Key ARN
		,@source_server CHAR(100) -- Enter your Production instance endpoint name
		,@target_server CHAR(100) -- Enter your DR instance endpoint name
)
/***********Author - Rajib Sadhu**************/

AS
BEGIN
		SET NOCOUNT ON

		DECLARE @LiCounter INT = 1
		DECLARE @LiMaxCount INT
		DECLARE @cmd NVARCHAR(4000)
		DECLARE @LsFileName NVARCHAR(260)
		DECLARE @BackupDir NVARCHAR(4000)
		DECLARE @PhysicalDeviceName [nvarchar](260)
		
		CREATE TABLE #tblLSTracking(
			[tracking_id] [int] identity(1,1),
			[source_server] [char](100) NOT NULL,
			[target_server] [char](100) NOT NULL,
			[database_name] [nvarchar](128) NOT NULL,
			[backup_start_date] [datetime] NULL,
			[backup_finish_date] [datetime] NULL,
			[backup_type] [varchar](8) NOT NULL,
			[backup_size] [numeric](20, 0) NULL,
			[physical_device_name] [nvarchar](260) NULL,
			[file_name] [nvarchar](260) NOT NULL,
			[backupset_name] [nvarchar](128) NULL,
			[processing_status] [varchar](30) NULL,
			[backup_set_id] [int] NULL,
			[checkpoint_lsn] [numeric](25, 0) NULL,
			[database_backup_lsn] [numeric](25, 0) NULL,
			[db_id] [int] NULL,
			[family_guid] [uniqueidentifier] NULL,
			[rds_backup_seq_id] [int] NULL,
			[backup_file_epoch] [bigint] NULL,
			[starting_lsn] [numeric](25, 0) NULL,
			[ending_lsn] [numeric](25, 0) NULL,
			[last_lsn] [numeric](25, 0) NULL
		) 

		/********************Pull all the tran log records not processed***************/

		INSERT INTO #tblLSTracking
		(
			[source_server]
			,[target_server]
			,[database_name]
			,[backup_start_date]
			,[backup_finish_date]
			,[backup_type]
			,[backup_size]
			,[physical_device_name]
			,[file_name]
			,[backupset_name]
			,[processing_status]
			,[backup_set_id]
			,[checkpoint_lsn]
			,[database_backup_lsn]
			,[db_id]
			,[family_guid]
			,[rds_backup_seq_id]
			,[backup_file_epoch]
			,[starting_lsn]
			,[ending_lsn]
			,[last_lsn]
		 )
		 
		SELECT [source_server]
			  ,[target_server]
			  ,[database_name]
			  ,[backup_start_date]
			  ,[backup_finish_date]
			  ,[backup_type]
			  ,[backup_size]
			  ,[physical_device_name]
			  ,[file_name]
			  ,[backupset_name]
			  ,[processing_status]
			  ,[backup_set_id]
			  ,[checkpoint_lsn]
			  ,[database_backup_lsn]
			  ,[db_id]
			  ,[family_guid]
			  ,[rds_backup_seq_id]
			  ,[backup_file_epoch]
			  ,[starting_lsn]
			  ,[ending_lsn]
			  ,[last_lsn]

		FROM	[TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] 
		WHERE	[source_server] = @source_server 
		AND		[target_server] = @target_server 
		AND		[database_name] = @DatabaseName 
		AND		[backup_type] = 'Log'
		AND		[processing_status] = 'copy-in-progress'
		ORDER BY [rds_backup_seq_id] ASC


		/******************************Restore Tran Log Backup*******************************/
		SET @LiCounter = 1
		
		WHILE(1=1)
		BEGIN
				SELECT @LiMaxCount = MAX([tracking_id]) FROM #tblLSTracking

				IF(@LiCounter <= @LiMaxCount)
				BEGIN
						SELECT @LsFileName = [file_name], @PhysicalDeviceName = physical_device_name FROM #tblLSTracking WHERE [tracking_id] = @LiCounter

						SET @BackupDir = '''' + @SecondaryServerS3BucketARN + '/' + LTRIM(RTRIM(@PhysicalDeviceName)) + '/' + LTRIM(RTRIM(@LsFileName)) + ''''
			
						SET @cmd = 'exec msdb.dbo.rds_restore_log @restore_db_name=' + '''' +  @DatabaseName + '''' + ', @s3_arn_to_restore_from=' + @BackupDir + ', @with_norecovery=1' + ', @kms_master_key_arn=' + '''' + LTRIM(RTRIM(@KMS_Key_ARN)) + '''' + ';' 						
							
						EXEC (@cmd)

						UPDATE	[TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] 
						SET		[processing_status] = 'restore-in-progress'
						WHERE	[source_server] = @source_server 
						AND		[target_server] = @target_server 
						AND		[database_name] = @DatabaseName 
						AND		[backup_type] = 'Log'
						AND		[file_name] = @LsFileName				

						SET @LiCounter = @LiCounter +1
				END
				ElSE
				BEGIN
						BREAK
				END

		END --WHILE(1=1)


		DROP TABLE #tblLSTracking


		SET NOCOUNT OFF

END
GO
/****** Object:  StoredProcedure [dbo].[uspManageSecondarySetSecondary]    Script Date: 11/19/2022 11:48:18 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[uspManageSecondarySetSecondary]
(
		@ListofDBs NVARCHAR(MAX) -- Enter your database name as comma separated
		,@SecondaryServerS3BucketARN NVARCHAR(500) -- Pass the S3 Bucket ARN of the Secondary Server
		,@SecondaryServerRDSAdminUser NVARCHAR(100) -- Enter Secondary Server RDS Admin user name
		,@LogRestoreFrequency SMALLINT --Enter how frequently you want to restore tran logs
		,@KMS_Key_ARN NVARCHAR(4000) -- Enter your Multi-Region KMS Key ARN
		,@PrimaryServer CHAR(100) -- Enter your Production instance endpoint name
		,@SecondaryServer CHAR(100) -- Enter your DR instance endpoint name	
		
)
/***********Author - Rajib Sadhu**************/
/*
EXEC dbo.uspManageSecondarySetSecondary 'AdventureWorks2019,AdventureWorksDW2019', 'arn:aws:s3:::rds-sql-server-crdr-us-east-2-bucket', 'admin', 15, 'rds-sql-server-crdr-us-west-2-instance', 'rds-sql-server-crdr-us-east-2-instance'
*/
AS
BEGIN
		SET NOCOUNT ON
		
		DECLARE @cmd NVARCHAR(4000)
		DECLARE @LiCounter INT = 1
		DECLARE @LiMaxCount INT
		DECLARE @LsDatabaseName SYSNAME
		DECLARE @jobId BINARY(16)
		DECLARE @JobName NVARCHAR(1000)
		DECLARE @BackupDir NVARCHAR(4000)
		DECLARE @LSBackUpScheduleUID   UNIQUEIDENTIFIER 
		DECLARE @LSBackUpScheduleID    INT 
		

		/*****************Parse the comma separated database names****************/

		CREATE TABLE #DBList
		(
			DatabaseId	INT IDENTITY(1,1),
			DatabaseName SYSNAME
		)

		DECLARE @DbListXML XML = CAST('<root><U>'+ Replace(@ListofDBs, ',', '</U><U>')+ '</U></root>' AS XML)
    
		INSERT INTO #DBList (DatabaseName)

		SELECT f.x.value('.', 'SYSNAME') AS user_id
		FROM @DbListXML.nodes('/root/U') f(x)
    
		/***************Make sure DB List does not have any system databases***********************/
		IF EXISTS(SELECT 1 FROM #DBList WHERE DatabaseName IN ('master','model','msdb','tempdb','rdsadmin','ssisdb','crdrdb'))
		BEGIN
			RAISERROR('Please remove system database/s from the list to proceed.', 16, 1)
			RETURN 1
		END

		/****************Check if the database specified exists***********************************/
		IF EXISTS (SELECT DatabaseName from #DBList WHERE DatabaseName IN (SELECT name FROM master.sys.databases))
		BEGIN
		RAISERROR('One of more databases in the list already present. Restore operation is cancelled.', 16, 1)
		RETURN 1
		END

		
		/******************************Restore Full Backup*******************************/
		SET @LiCounter = 1
		
		WHILE(1=1)
		BEGIN
				SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

				IF(@LiCounter <= @LiMaxCount)
				BEGIN
						SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter

						SET @BackupDir = '''' + @SecondaryServerS3BucketARN + '/' + LOWER(LTRIM(RTRIM(@LsDatabaseName))) + '_fullbackup.bak' + ''''
			
						SET @cmd = 'exec msdb.dbo.rds_restore_database @restore_db_name=' + '''' +  @LsDatabaseName + '''' + ', @s3_arn_to_restore_from=' + @BackupDir + ', @type=''FULL'', @with_norecovery=1;'
						
						EXEC (@cmd)

						UPDATE	[TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] 
						SET		[processing_status] = 'Processed'
						WHERE	[source_server] = @PrimaryServer 
						AND		[target_server] = @SecondaryServer 
						AND		[database_name] = @LsDatabaseName 
						AND		[backup_type] = 'Database'
						AND		[processing_status] IS NULL					

						SET @LiCounter = @LiCounter +1
				END
				ElSE
				BEGIN
						BREAK
				END

		END --WHILE(1=1)
		

		/*******************************Tran Log Restore Job Creation******************************/
		
		SET @LiCounter = 1

		WHILE(1=1)
		BEGIN
				SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

				IF(@LiCounter <= @LiMaxCount)
				BEGIN
						SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter

						SET @JobName = 'LSRestore_' + @LsDatabaseName
						
						SET @cmd = 'EXEC [crdrdb].[dbo].[uspManageSecondaryRestoreLogs] ' + '''' + @LsDatabaseName + '''' + ', ' + '''' + LTRIM(RTRIM(@SecondaryServerS3BucketARN)) + '''' + ', ' + '''' + LTRIM(RTRIM(@KMS_Key_ARN)) + '''' + ', ' + '''' + LTRIM(RTRIM(@PrimaryServer)) + '''' + ', ' +  '''' + LTRIM(RTRIM(@SecondaryServer)) + '''' + ';'

						SET @jobId = NULL
						SET @LSBackUpScheduleUID   = NULL 
						SET @LSBackUpScheduleID    = NULL 
																
						EXEC  msdb.dbo.sp_add_job @job_name=@JobName, 
								@enabled=1, 
								@notify_level_eventlog=0, 
								@notify_level_email=2, 
								@notify_level_page=2, 
								@delete_level=0, 
								@category_name=N'[Uncategorized (Local)]', 
								@owner_login_name=@SecondaryServerRDSAdminUser, @job_id = @jobId OUTPUT

						EXEC msdb.dbo.sp_add_jobserver @job_name=@JobName, @server_name = @@SERVERNAME

						EXEC msdb.dbo.sp_add_jobstep @job_name=@JobName, @step_name=N'tran log restore', 
								@step_id=1, 
								@cmdexec_success_code=0, 
								@on_success_action=1, 
								@on_fail_action=2, 
								@retry_attempts=0, 
								@retry_interval=0, 
								@os_run_priority=0, @subsystem=N'TSQL', 
								@command=@cmd, 
								@database_name=N'master', 
								@flags=0


						EXEC msdb.dbo.sp_add_schedule 
									@schedule_name =N'LSRestoreSchedule' 
									,@enabled = 1 
									,@freq_type = 4 
									,@freq_interval = 1 
									,@freq_subday_type = 4 
									,@freq_subday_interval = @LogRestoreFrequency 
									,@freq_recurrence_factor = 0 
									,@active_start_date = 20100101 
									,@active_end_date = 99991231 
									,@active_start_time = 0 
									,@active_end_time = 235900 
									,@schedule_uid = @LSBackUpScheduleUID OUTPUT 
									,@schedule_id = @LSBackUpScheduleID OUTPUT 

							EXEC msdb.dbo.sp_attach_schedule 
									@job_id = @jobId 
									,@schedule_id = @LSBackUpScheduleID  
						

						SET @LiCounter = @LiCounter +1
				END
				ElSE
				BEGIN
						BREAK
				END

		END --WHILE(1=1)
		
		
		/*******************************Create LSTracking-Secondary  Job******************************/

		SET @JobName = 'LSTracking-Secondary'
		
		SET @cmd = 'EXEC [crdrdb].[dbo].[uspManageSecondaryLSTracking] ' + '''' + CAST(@ListofDBs AS NVARCHAR(4000)) + '''' + ', ' + '''' + LTRIM(RTRIM(@PrimaryServer)) + '''' + ', ' +  '''' + LTRIM(RTRIM(@SecondaryServer)) + '''' + ';'

		SET @jobId = NULL
		SET @LSBackUpScheduleUID   = NULL 
		SET @LSBackUpScheduleID    = NULL 
																
		EXEC  msdb.dbo.sp_add_job @job_name=@JobName, 
				@enabled=1, 
				@notify_level_eventlog=0, 
				@notify_level_email=0, 
				@notify_level_netsend=0, 
				@notify_level_page=0, 
				@delete_level=0, 
				@description=N'No description available.', 
				@category_name=N'[Uncategorized (Local)]',  
				@owner_login_name=@SecondaryServerRDSAdminUser, @job_id = @jobId OUTPUT

		EXEC msdb.dbo.sp_add_jobserver @job_name=@JobName, @server_name = @@SERVERNAME

		EXEC msdb.dbo.sp_add_jobstep @job_name=@JobName, @step_name=N'Track secondary LS Restore', 
				@step_id=1, 
				@cmdexec_success_code=0, 
				@on_success_action=1, 
				@on_success_step_id=0, 
				@on_fail_action=2, 
				@on_fail_step_id=0, 
				@retry_attempts=0, 
				@retry_interval=0, 
				@os_run_priority=0, @subsystem=N'TSQL', 
				@command=@cmd, 
				@database_name=N'master', 
				@flags=0							

		EXEC msdb.dbo.sp_add_schedule 
				@schedule_name =N'run every 5 minutes' 
				,@enabled = 1 
				,@freq_type = 4 
				,@freq_interval = 1 
				,@freq_subday_type = 4 
				,@freq_subday_interval = 5				
				,@freq_relative_interval=0 
				,@freq_recurrence_factor = 0 
				,@active_start_date = 20100101 
				,@active_end_date = 99991231 
				,@active_start_time = 0 
				,@active_end_time = 235900 
				,@schedule_uid = @LSBackUpScheduleUID OUTPUT 
				,@schedule_id = @LSBackUpScheduleID OUTPUT 

		EXEC msdb.dbo.sp_attach_schedule 
				@job_id = @jobId 
				,@schedule_id = @LSBackUpScheduleID 



		DROP TABLE #DBList

		SET NOCOUNT OFF
END
GO

/****** Object:  StoredProcedure [dbo].[uspManageSecondaryPromote]    Script Date: 1/1/2022 8:51:38 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[uspManageSecondaryPromote]
(
		@ListofDBs NVARCHAR(MAX) -- Enter your database name as comma separated	
		,@source_server CHAR(100) -- Enter your Production instance endpoint name
		,@target_server CHAR(100) -- Enter your DR instance endpoint name
)
/***********Author - Rajib Sadhu**************/
/*
EXEC dbo.uspManageSecondaryCutover 'AdventureWorks2019,AdventureWorksDW2019,AdventureWorksLT2019','rds-sql-server-crdr-us-west-2-instance','rds-sql-server-crdr-us-east-2-instance'
*/
AS
BEGIN
	SET NOCOUNT ON

		DECLARE @cmd NVARCHAR(4000)
		DECLARE @LiCounter INT = 1
		DECLARE @LiMaxCount INT
		DECLARE @LsDatabaseName SYSNAME
		DECLARE @JobName NVARCHAR(1000)

		/*****************Parse the comma separated database names****************/

		CREATE TABLE #DBList
		(
			DatabaseId	INT IDENTITY(1,1),
			DatabaseName SYSNAME
		)

		DECLARE @DbListXML XML = CAST('<root><U>'+ Replace(@ListofDBs, ',', '</U><U>')+ '</U></root>' AS XML)
    
		INSERT INTO #DBList (DatabaseName)

		SELECT f.x.value('.', 'SYSNAME') AS user_id
		FROM @DbListXML.nodes('/root/U') f(x)

		/****************Check if the database specified exists***********************************/
		IF EXISTS (SELECT DatabaseName from #DBList WHERE DatabaseName NOT IN (SELECT name FROM master.sys.databases))
		BEGIN
		RAISERROR('One of more databases is not present. Promote operation is cancelled.', 16, 1)
		RETURN 1
		END

		/*******************************Drop Tran Log Restore Jobs***************************/
		

		IF EXISTS(SELECT 1 FROM [TrackingDBServer].[trackingdb].[dbo].[tblLSTracking] WHERE [source_server] = @source_server AND	[target_server] = @target_server AND [processing_status] <> 'Processed' AND [database_name] IN (SELECT DatabaseName from #DBList))
		BEGIN
				RAISERROR('Latest Transaction Logs are not applied for one or more databases in the list. Cutover operation is aborted.', 16, 1)
				RETURN 1
		END

		SET @LiCounter = 1

		WHILE(1=1)
		BEGIN
				SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

				IF(@LiCounter <= @LiMaxCount)
				BEGIN
						SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter

						SET @JobName = 'LSRestore_' + @LsDatabaseName

						SET @cmd = 'EXEC msdb..sp_delete_job @job_name = ' + '''' +  @JobName + '''' 
						
						EXEC (@cmd)	

						SET @LiCounter = @LiCounter +1
				END
				ElSE
				BEGIN
						BREAK
				END

		END --WHILE(1=1)

		/******************************Finish Restore***********************************************/
		SET @LiCounter = 1
		
		WHILE(1=1)
		BEGIN
				SELECT @LiMaxCount = MAX(DatabaseId) FROM #DBList

				IF(@LiCounter <= @LiMaxCount)
				BEGIN
						SELECT @LsDatabaseName = DatabaseName FROM #DBList WHERE DatabaseId = @LiCounter

						SET @cmd = 'EXECUTE msdb.dbo.rds_finish_restore ' + '''' +  @LsDatabaseName + '''' 
						
						EXEC (@cmd)						

						SET @LiCounter = @LiCounter +1
				END
				ElSE
				BEGIN
						BREAK
				END

		END --WHILE(1=1)

	SET NOCOUNT OFF
END
GO