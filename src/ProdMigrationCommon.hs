{-# LANGUAGE OverloadedStrings, RecordWildCards #-}
module ProdMigrationCommon where

import Data.Text
import Util
import Model
import qualified Data.List as DL
import Data.Maybe
import Control.Exception
import Database.MySQL.Base

cookie :: Text
cookie = "LambdaKnight=AqkG7OnBArYQCXrJsxFseRbV4gY3D3QEpKWIrZJF"

get_table_create_info :: (Integer, Integer) -> IO TableConfig
get_table_create_info (prod_schemaId, tableId) = do
  putStrLn $ (show prod_schemaId) ++ ", " ++ (show tableId)
  (tableName, structure, shardRules ) <- get_table_info tableId
  let (columns', indices') = either (\e -> error $ show tableId ++ " > " ++ show e) Prelude.id $
                               parse_create_table structure
  shard <- is_schema_shard prod_schemaId
  dev_schemaId <- get_dev_schema_id prod_schemaId
  let shardConfig = if shard then
        let shardRules' = DL.head . read $ unpack shardRules :: Text
        in Just (fromJust $ either (const Nothing) Just $ get_shard_config shardRules')
        else Nothing
  -- putStrLn $ show shardConfig
  let columns = DL.map col_2_col_ columns'
      indices = DL.map index_2_index_ indices'
      schemaId = dev_schemaId
  return TableConfig {..}
  where
    get_dev_schema_id psi = do
    bracket new_conn close $ \c -> do
      ps <- prepareStmt c "SELECT DailyID FROM SchemaCorrelation WHERE ProdID = ?"
      rows <- query_rows_stmt c ps [toMySQLValue psi]
      return $ getInt . DL.head . DL.head $ rows

get_table_info :: Integer -> IO (Text, Text, Text)
get_table_info tid = do
  bracket prod_conn close $ \c -> do
    ps <- prepareStmt c "SELECT Name, Structure, ShardRules FROM TableMeta WHERE ID = ?"
    rows <- query_rows_stmt c ps [toMySQLValue tid]
    return . (\[a,b,c] -> (getText a, getText b, getText c)) . DL.head $ rows

is_schema_shard :: Integer -> IO Bool
is_schema_shard schema_id = do
  bracket prod_conn close $ \c -> do
    ps <- prepareStmt c "SELECT Shard FROM SchemaMeta WHERE ID = ?"
    rows <- query_rows_stmt c ps [toMySQLValue schema_id]
    return . (== 1) . getInt . DL.head . DL.head $ rows
