IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'metadata')
    EXEC('CREATE SCHEMA metadata');
GO

IF OBJECT_ID('dbo.FactStream', 'U') IS NOT NULL DROP TABLE dbo.FactStream;
IF OBJECT_ID('dbo.DimTrack', 'U') IS NOT NULL DROP TABLE dbo.DimTrack;
IF OBJECT_ID('dbo.DimArtist', 'U') IS NOT NULL DROP TABLE dbo.DimArtist;
IF OBJECT_ID('dbo.DimDate', 'U') IS NOT NULL DROP TABLE dbo.DimDate;
IF OBJECT_ID('dbo.DimUser', 'U') IS NOT NULL DROP TABLE dbo.DimUser;
GO

CREATE TABLE dbo.DimUser (
    user_id INT NOT NULL PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL,
    country VARCHAR(255) NOT NULL,
    subscription_type VARCHAR(50) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NULL,
    updated_at DATETIME2(0) NOT NULL
);
GO

CREATE TABLE dbo.DimArtist (
    artist_id INT NOT NULL PRIMARY KEY,
    artist_name VARCHAR(255) NOT NULL,
    genre VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    updated_at DATETIME2(0) NOT NULL
);
GO

CREATE TABLE dbo.DimTrack (
    track_id INT NOT NULL PRIMARY KEY,
    track_name VARCHAR(255) NOT NULL,
    artist_id INT NOT NULL,
    album_name VARCHAR(255) NOT NULL,
    duration_sec INT NOT NULL,
    release_date DATE NOT NULL,
    updated_at DATETIME2(0) NOT NULL,
    CONSTRAINT FK_DimTrack_DimArtist FOREIGN KEY (artist_id) REFERENCES dbo.DimArtist(artist_id)
);
GO

CREATE TABLE dbo.DimDate (
    date_key INT NOT NULL PRIMARY KEY,
    [date] DATE NOT NULL,
    [day] INT NOT NULL,
    [month] INT NOT NULL,
    [year] INT NOT NULL,
    weekday VARCHAR(20) NOT NULL
);
GO

CREATE TABLE dbo.FactStream (
    stream_id BIGINT NOT NULL PRIMARY KEY,
    user_id INT NOT NULL,
    track_id INT NOT NULL,
    date_key INT NOT NULL,
    listen_duration INT NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    stream_timestamp DATETIME2(0) NOT NULL,
    CONSTRAINT FK_FactStream_DimUser FOREIGN KEY (user_id) REFERENCES dbo.DimUser(user_id),
    CONSTRAINT FK_FactStream_DimTrack FOREIGN KEY (track_id) REFERENCES dbo.DimTrack(track_id),
    CONSTRAINT FK_FactStream_DimDate FOREIGN KEY (date_key) REFERENCES dbo.DimDate(date_key)
);
GO

