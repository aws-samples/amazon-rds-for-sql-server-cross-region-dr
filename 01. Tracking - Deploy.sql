USE [master]
GO
CREATE DATABASE [trackingdb]
GO

USE [trackingdb]
GO

/****** Object:  Table [dbo].[tblLSTracking]    Script Date: 11/19/2022 9:55:14 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[tblLSTracking](
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
) ON [PRIMARY]
GO
