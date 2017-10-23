{-# LANGUAGE OverloadedStrings, RecordWildCards #-}
module ProdSchemaMigration where

import ProdMigrationCommon
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

setup_schemaCorrelation :: IO ()
setup_schemaCorrelation = do
  let sql = "select a.ID as prodID, b.ID as qaID from prod_console.SchemaMeta a \
            \ left join (select ID, Name, Shard FROM new_console.SchemaMeta \
            \ WHERE Env = 'QA') b on a.Name = b.Name AND a.Shard = b.Shard \
            \ WHERE a.ID NOT IN(177,157,181, 200, 231, 124) AND (b.ID NOT IN(795, 796, 786, 818, 956, 1019) \
            \ OR b.ID is NULL) AND b.ID is NOT NULL;"
  rows <- bracket new_conn close $ (\c -> query_rows c sql)
  let items0 = DL.map (\[prodid, qaid] -> (getInt prodid, getInt qaid)) rows
      items = (177, 796):(157, 795):(181, 786):(200, 818):(231, 956):(124, 1019):items0
  bracket new_conn close $ \c -> do
    ps <- prepareStmt c "UPDATE new_console.SchemaCorrelation SET ProdID = ? WHERE QaID = ? "
    mapM_ (do_setup c ps) items
  where
    do_setup :: MySQLConn -> StmtID -> (Integer, Integer) -> IO ()
    do_setup c ps (pid, qid) = do
      executeStmt c ps [toMySQLValue pid, toMySQLValue qid]
      return ()

get_schema_prod_only :: IO [(Integer, Integer, Text)]
get_schema_prod_only = do
  let sql = "select a.ID, a.Shard, a.Name from prod_console.SchemaMeta a \
            \ left join (select ID, Name, Shard FROM new_console.SchemaMeta \
            \ WHERE Env = 'QA') b on a.Name = b.Name AND a.Shard = b.Shard \
            \ WHERE a.ID NOT IN(177,157,181, 200, 231, 124) AND (b.ID NOT IN(795, 796, 786, 818, 956, 1019) \
            \ OR b.ID is NULL) AND b.ID is NULL;"
  rows <- bracket new_conn close $ (\c -> query_rows c sql)
  let items = DL.map (\[a, b, c] -> (getInt a, getInt b, getText c)) rows
  return items

get_schema_need_qa_sync :: IO [(Integer, Integer, Text, Integer)]
get_schema_need_qa_sync = do
  prod_only <- get_schema_prod_only
  let pid_str = DL.intercalate "," $ DL.map (\(a, b, c) -> show a) prod_only
  let sql = "select a.ID, a.Shard, a.Name, b.ID from prod_console.SchemaMeta a \
            \ left join (select ID, Name, Shard FROM new_console.SchemaMeta \
            \ WHERE Env = 'DAILY') b on a.Name = b.Name AND a.Shard = b.Shard \
            \ WHERE a.ID IN(" ++ pid_str ++ ");"
  Prelude.putStrLn sql
  rows <- bracket new_conn close $ (\c -> query_rows c $ Query . fromStrict . encodeUtf8 $ Data.Text.pack sql)
  let items = DL.map (\[a, b, c, d] -> (getInt a, getInt b, getText c, getInt d)) rows
  return items

create_schema :: Integer -> IO (Either Text Integer)
create_schema prod_id = do
  si@SchemaInfo{..} <- bracket prod_conn close $ \c -> get_schema_info c prod_id
  let schema_info = si { clusterId = if shardCount > 1 then 3 else 2
                       , instanceGroupId = if shardCount > 1 then 7 else 6 }
  submit_ddl_operate cookie
    "http://pre-rds.qima-inc.com/api/ddl/operate/daily/plan/create_schema"
    schema_info

sync_schema :: Integer -> IO (Either Text Integer)
sync_schema dev_id = do
  si@SchemaInfo{..} <- bracket new_conn close $ \c -> get_schema_info c dev_id
  let schema_info = si { clusterId = if shardCount > 1 then 14 else 16
                       , instanceGroupId = if shardCount > 1 then 31 else 33
                       , syncSchemaId = dev_id }
  submit_ddl_operate cookie
    "http://pre-rds.qima-inc.com/api/ddl/operate/qa/plan/create_schema"
    schema_info
