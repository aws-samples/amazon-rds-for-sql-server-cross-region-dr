DECLARE @TrackingServerEndpoint NVARCHAR(500)
DECLARE @TrackingAdminUser NVARCHAR(100) 
DECLARE @TrackingAdminPassword NVARCHAR(100)

SET @TrackingServerEndpoint = 'rds-sql-server-crdr-tracking.xxxxx.us-east-1.rds.amazonaws.com'
SET @TrackingAdminUser = 'Admin'
SET @TrackingAdminPassword = '*******'

IF NOT EXISTS(SELECT 1 FROM sys.servers WHERE name = 'TrackingDBServer' AND is_linked = 1)
		BEGIN

			EXEC master.dbo.sp_addlinkedserver @server = N'TrackingDBServer', @srvproduct=N'', @provider=N'SQLNCLI', @datasrc=@TrackingServerEndpoint;

			EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=N'TrackingDBServer',@useself=N'False',@locallogin=NULL,@rmtuser=@TrackingAdminUser,@rmtpassword=@TrackingAdminPassword;

			
		END
