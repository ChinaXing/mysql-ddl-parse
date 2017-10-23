{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Model( ShardConfig(..)
            , DataType(..)
            , DataType_(..)
            , Col(..)
            , Col_(..)
            , Index(..)
            , Index_(..)
            , CreateTable(..)
            , Production(..)
            , CreateTableInfo(..)
            , TableConfig(..)
            , SchemaInfo(..)
            , DelTableInfo(..)
            , SyncTableConfig(..)
            , SyncTableInfo(..)
            , DtC(..)
            , HdT(..)
            , index_2_index_
            , col_2_col_
            , col_name
            , idx_name
            ) where

import qualified Data.Text as DT
import Data.Aeson
import GHC.Generics

instance ToJSON Production
instance FromJSON Production
instance ToJSON SchemaInfo
instance ToJSON SyncTableConfig
instance ToJSON SyncTableInfo

dt_2_dt_ :: DataType -> DataType_
dt_2_dt_ DataType{..} = DataType_{..}

col_2_col_ :: Col -> Col_
col_2_col_ Col{..} = Col_ { idNum = idNum, name = name, dataType = dt_2_dt_ dataType
                          , pk = pk, autoIncrement = autoIncrement
                          , null = null, defaultValue = defaultValue, updateDefaultValue = updateDefaultValue
                          , comment = comment }

index_2_index_ :: Index -> Index_
index_2_index_ Index{..} = Index_{..}

data Production = Production
  { id :: Integer, name :: DT.Text } deriving(Eq, Show, Generic)

data SchemaInfo = SchemaInfo
  { syncSchemaId :: Integer
  , shard :: Bool
  , products :: [Production]
  , schemaUser :: DT.Text
  , dbUser :: DT.Text
  , maxConnPerInstance :: Integer
  , shardConnPoolSize :: Integer
  , shardMaxConn :: Integer
  , shardCount :: Integer
  , rwPolicy :: DT.Text
  , instanceGroupId :: Integer
  , clusterId :: Integer
  , schemaName :: DT.Text
  , title :: DT.Text
  } deriving(Eq, Show, Generic)

data CreateTableInfo = CreateTableInfo
  { title :: DT.Text
  , tableConfig :: [TableConfig]
  } deriving(Show,Eq, Generic)


data SyncTableConfig = SyncTableConfig
  { tableId :: Int
  , version :: Int
  , versionId :: Int
  , ddl :: DT.Text
  } deriving(Show, Eq, Generic)

data SyncTableInfo = SyncTableInfo
  { title:: DT.Text
  , tableConfig :: [SyncTableConfig]
  } deriving(Show, Eq, Generic)

data DataType_ = DataType_
  { t :: String
  , len :: Maybe Int
  , unsigned :: Bool
  } deriving(Eq, Show, Generic)

data Col_ = Col_
  { idNum :: Int
  , name :: String
  , dataType :: DataType_
  , pk :: Bool
  , autoIncrement :: Bool
  , null :: Bool
  , defaultValue :: Maybe (Maybe String)
  , updateDefaultValue :: Maybe String
  , comment :: Maybe String
  } deriving(Eq, Show)

data Index_ = Index_
  { idNum :: Int
  , name :: String
  , columns :: [String]
  , unique :: Bool
  , pk :: Bool
  , comment :: Maybe String
  } deriving(Eq, Show)

data TableConfig = TableConfig
  { tableName :: DT.Text
  , schemaId :: Integer
  , columns :: [Col_]
  , indices :: [Index_]
  , shardConfig :: Maybe ShardConfig
  } deriving(Eq, Show, Generic)

data DelTableInfo = DelTableInfo
  { title :: DT.Text
  , tableConfig :: [DtC]
  } deriving(Show, Eq, Generic)

data DtC = DtC
  { tableId :: Integer
  , schemaId :: Integer
  , hardDropTime :: HdT
  } deriving(Show, Eq, Generic)

data HdT = HdT
  {delay :: Integer
  ,timeMinutes :: Integer
  } deriving(Show, Eq,Generic)

data ShardConfig = ShardConfig
  { column :: DT.Text
  , itype :: DT.Text
  , slice :: Maybe [Integer]
  } deriving(Eq, Show, Generic)

data DataType = DataType
  { t :: String
  , len :: Maybe Int
  , unsigned :: Bool
  } deriving(Eq, Show, Generic)

data Col = Col
  { idNum :: Int
  , name :: String
  , dataType :: DataType
  , pk :: Bool
  , autoIncrement :: Bool
  , null :: Bool
  , defaultValue :: Maybe (Maybe String)
  , updateDefaultValue :: Maybe String
  , comment :: Maybe String
  } deriving(Eq, Show, Generic)

data Index = Index
  { idNum :: Int
  , name :: String
  , columns :: [String]
  , unique :: Bool
  , pk :: Bool
  , comment :: Maybe String
  } deriving(Eq, Show, Generic)

data CreateTable = CreateTable
  { tableName :: String
  , columns :: [Col]
  , indices :: [Index]
  } deriving(Eq, Show, Generic)


col_name Col{..} = name
idx_name Index{..} = name
