{-# LANGUAGE OverloadedStrings, RecordWildCards #-}
module ProdMigration where

import Util
import Model
import DDL
import Data.Text
import qualified Data.Text.IO as TIO
import Data.Text.Encoding
import Data.Text.Format
import qualified Data.Text.Lazy as DTL
import Data.ByteString.Lazy
import qualified Data.List as DL
import Control.Exception
import Control.Monad
import Control.Applicative
import Data.Monoid
import Data.Maybe
import TextShow
import Database.MySQL.Base

import qualified ProdSchemaMigration as SM
import qualified ProdTableMigration as TM

meta_table_list = [ ("Cluster", 50)
                  , ("ClusterConfig", 4000)
                  , ("Proxy", 50)
                  , ("ProxyServerConfig",50)
                  , ("InstanceGroup", 50)
                  , ("HaGroup", 50)
                  , ("Shards", 1500)
                  , ("Shard", 40000)
                  , ("SchemaMeta", 1500)
                  , ("TableRule",1500)
                  , ("TableMeta", 41000)
                  , ("ShardFunction",100)
                  , ("MySQLInstance",100)
                  , ("MySQLHost",50)
                  , ("SchemaAccount", 1500)
                  , ("TableVersionMeta", 8000)
                  , ("TableVersionTrack", 18000)]

-- Keep: prod meta data not overlap with qa meta data
modify_prod_with_delta :: IO ()
modify_prod_with_delta = do
  -- for each table, get maxID from new_console , get delta, update ID in prod_console
  meta_max_id <- bracket new_conn close $
    \new_c -> mapM (get_meta_max_id new_c . fst) meta_table_list
  when (not $ DL.all (> 0) meta_max_id) $ error "get max id failure"
  let meta_offset = DL.map snd meta_table_list
  let [ cluster_offset, clusterConfig_offset
        , proxy_offset, proxyServerConfig_offset
        , instanceGroup_offset, haGroup_offset
        , shards_offset, shard_offset
        , schemaMeta_offset, tableRule_offset
        , tableMeta_offset, shardFunction_offset
        , mySQLInstance_offset, mySQLHost_offset
        , schemaAccount_offset, tableVersionMeta_offset
        , tableVersionTrack_offset ] = meta_offset
  let gt0 = DL.all (> 0) $ DL.zipWith (-)  meta_offset meta_max_id
  when (not gt0) $ do
    mapM_ (Prelude.putStrLn . show) $ DL.zipWith (,) meta_offset meta_max_id
    error "offset not great than Zero, please Fix it !"
  Prelude.putStrLn "get delta ok ..."
  let fix_SQL =
        [ format "UPDATE Cluster SET ID = ID + {} ORDER BY ID DESC" (Only cluster_offset)
        , format "UPDATE MySQLHost SET ID = ID + {} ORDER BY ID DESC " (Only mySQLHost_offset)
        , format "UPDATE MySQLInstance SET ID = ID + {}, MySQLHostID = MySQLHostID + {}, \
                 \ HaGroupID = HaGroupID + {}, InstanceGroupID = InstanceGroupID + {} ORDER BY ID DESC"
          (mySQLInstance_offset, mySQLHost_offset, haGroup_offset, instanceGroup_offset)
        , format "UPDATE InstanceGroup SET ID = ID + {} ORDER BY ID DESC" (Only instanceGroup_offset)
        , format "UPDATE HaGroup SET ID = ID + {}, InstanceGroupID = InstanceGroupID + {} ORDER BY ID DESC"
          (haGroup_offset, instanceGroup_offset)
        , format "UPDATE SchemaMeta SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (schemaMeta_offset, cluster_offset)
        , format "UPDATE SchemaAccount SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (schemaAccount_offset, cluster_offset)
        , format "UPDATE TableMeta SET ID = ID + {}, SchemaID = SchemaID + {}, \
                 \ ClusterID = ClusterID + {} ORDER BY ID DESC"
          (tableMeta_offset, schemaMeta_offset, cluster_offset)
        , format "UPDATE TableRule SET ID = ID + {}, ClusterID = ClusterID + {}, \
                 \ AlgFunctionId = AlgFunctionId + {} ORDER BY ID DESC"
          (tableRule_offset, cluster_offset, shardFunction_offset)
        , format "UPDATE ShardFunction SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (shardFunction_offset, cluster_offset)
        , format "UPDATE Shard SET ID = ID + {}, ClusterID = ClusterID + {}, \
                 \ HaGroupID = HaGroupID + {}, ShardsID = SHardsID + {} ORDER BY ID DESC"
          (shard_offset, cluster_offset, haGroup_offset, shards_offset)
        , format "UPDATE Shards SET ID = ID + {}, ClusterID = ClusterID + {}, \
                 \ InstanceGroupID = InstanceGroupID + {}, SchemaID = SchemaID + {} ORDER BY ID DESC"
          (shards_offset, cluster_offset, instanceGroup_offset, schemaMeta_offset)
        , format "UPDATE Proxy SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (proxy_offset, cluster_offset)
        , format "UPDATE ProxyServerConfig SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (proxyServerConfig_offset, cluster_offset)
        , format "UPDATE ClusterConfig SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (clusterConfig_offset, cluster_offset)
          -- update ProdID field
        , format "UPDATE new_console.SchemaCorrelation SET ProdID = ProdID + {} WHERE ProdID != 0" (Only schemaMeta_offset)
        ]
  bracket  prod_conn close $ \prod_c -> do
    mapM_ (\sql -> do
              TIO.putStrLn sql
              execute_ prod_c . Query . fromStrict . encodeUtf8 $ sql
          ) $ DL.map (DTL.toStrict) fix_SQL
  -- update accounts field of SchemaMeta
  bracket prod_conn close $ \prod_c -> do
    rows <- query_rows prod_c "SELECT ID, Accounts FROM SchemaMeta"
    ps <- prepareStmt prod_c "UPDATE SchemaMeta SET Accounts = ? WHERE ID = ?"
    mapM_ (\[id, a] -> do
              let accounts = read . Data.Text.unpack . getText $ a :: [Integer]
                  accounts' = DL.map (+ schemaAccount_offset) accounts
              executeStmt prod_c ps [toMySQLValue . showt $ accounts', id]
          ) rows
  where
    get_meta_max_id :: MySQLConn -> Text -> IO Integer
    get_meta_max_id c t = do
      let sql =
            if t == "TableVersionMeta" then
              "SELECT MAX(RelativeID) FROM TableVersionMeta"
            else
              Query $ fromStrict $ encodeUtf8 $ "SELECT MAX(ID) FROM " <> t
      rows <- query_rows c sql
      let v = getIntOrNull $ DL.head $ DL.head rows
      when (isNothing v) $ error $ "cannot get max 'ID' of Table : " <> Data.Text.unpack t
      return $ fromJust v

run :: IO ()
run = do
  Prelude.putStrLn ">>>>> SETUP SchemaCorrelation >>>>>>> "
  SM.setup_schemaCorrelation
  Prelude.putStrLn ">>>>> GET SCHEMA Prod Only >>>>>>> "
  cnt <- SM.get_schema_prod_only >>= return . DL.length
  when (cnt /= 0) $ error $ "schema_prod_only count not equals Zero, is : " ++ (show cnt)
  Prelude.putStrLn ">>>>> MODIFY PROD With Delta >>>>>>> "
  modify_prod_with_delta
  Prelude.putStrLn ">>>>> SETUP TABLE Relative >>>>>>> "
  TM.setup_table_relative
  Prelude.putStrLn ">>>>> TABLE Prod QA Missing >>>>>>> "
  cnt <- TM.table_prod_qa_miss >>= return . DL.length
  when (cnt /= 0) $ error $ "prod table qa missing count not equals Zero, is : " ++ (show cnt)
  Prelude.putStrLn ">>>>> SETUP Meta BY Structure Equals >>>>>>> "
  TM.setup_meta_by_structure_cmp
  --Prelude.putStrLn ">>>>> SETUP Meta BY Prepend >>>>>>>"
  -- TM.setup_meta_by_prepend
  Prelude.putStrLn ">>>>> DONE. >>>>>>>"
